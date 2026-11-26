/*
 * Copyright (C) 2026 Sonic Liberation
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.github.sonic_liberation.spotube_plugin_spotify.core

import io.github.sonic_liberation.spotify_gql_client.gql.CredentialsFromCookieResult
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.addJsonObject
import org.spotube.plugin_interfaces.host_apis.HttpClientAPI
import org.spotube.plugin_interfaces.host_apis.HttpMethod
import org.spotube.plugin_interfaces.host_apis.PersistedStorageAPI
import org.spotube.plugin_interfaces.host_apis.WebSocketAPI
import org.spotube.plugin_interfaces.host_apis.WebSocketEvent
import org.spotube.plugin_interfaces.plugin_apis.device_control.DeviceControlAPI
import org.spotube.plugin_interfaces.plugin_apis.device_control.DeviceControlException
import org.spotube.plugin_interfaces.plugin_apis.device_control.RemoteDevice
import org.spotube.plugin_interfaces.plugin_apis.device_control.RemotePlaybackItem
import org.spotube.plugin_interfaces.plugin_apis.device_control.RemotePlaybackState
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.random.Random
import kotlin.time.Clock

private const val SPCLIENT = "https://spclient.wg.spotify.com"
private const val CANVAZ_URL = "$SPCLIENT/canvaz-cache/v0/canvases"
private const val MAX_LIST = 500
private const val BINARY_BODY_HEADER = "X-Zipline-Binary-Body"
private const val BINARY_BODY_VALUE = "base64"

/**
 * Spotify Connect (remote device control) and Spotify Canvas, both through this plugin's own
 * already-authenticated session - neither ships in the app itself; see [DeviceControlAPI]. Ported
 * from the app's former direct implementation, onto [HttpClientAPI] and [WebSocketAPI] instead of
 * a direct network client, since this code runs sandboxed inside Zipline.
 */
class RealDeviceControlAPI(
    private val httpClient: HttpClientAPI,
    private val webSocket: WebSocketAPI,
    private val storage: PersistedStorageAPI,
) : DeviceControlAPI {
    private val json = Json { ignoreUnknownKeys = true }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** Id of this plugin's hidden controller device, stable while the plugin runs. */
    private val controllerId = randomHex(40)

    private val connectionLock = Mutex()
    private var connectionId: String? = null
    private var dealerJob: Job? = null

    private var devicesCache: List<RemoteDevice> = emptyList()
    private var playbackCache: RemotePlaybackState? = null
    private var activeDeviceId: String? = null

    private val _changes = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    override fun changes(): Flow<Unit> = _changes.asSharedFlow()

    private val canvasCache = mutableMapOf<String, String?>()
    private val canvasMutex = Mutex()

    // --- auth -----------------------------------------------------------------------------

    private suspend fun accessToken(): String? {
        val raw = storage.getString("credentials") ?: return null
        val credentials = runCatching { json.decodeFromString<CredentialsFromCookieResult>(raw) }.getOrNull()
            ?: return null
        if (credentials.expiration <= Clock.System.now().toEpochMilliseconds()) return null
        return credentials.accessToken
    }

    override suspend fun isAvailable(): Boolean = accessToken() != null

    private suspend fun token(): String = accessToken() ?: throw DeviceControlException(401, "not logged in")

    private suspend fun authHeaders(vararg extra: Pair<String, String>): Map<String, String> =
        mapOf("Authorization" to "Bearer ${token()}") + extra

    private fun checkStatus(statusCode: Int, body: String?) {
        if (statusCode !in 200..299) throw DeviceControlException(statusCode, body.orEmpty())
    }

    // --- Canvas -----------------------------------------------------------------------------

    @OptIn(ExperimentalEncodingApi::class)
    override suspend fun canvasUrl(trackId: String): String? {
        if (!SPOTIFY_ID.matches(trackId)) return null
        canvasMutex.withLock { if (canvasCache.containsKey(trackId)) return canvasCache[trackId] }
        val token = accessToken() ?: return null
        val url = runCatching {
            val requestBytes = requestBody("spotify:track:$trackId")
            val response = httpClient.request(
                method = HttpMethod.Post,
                url = CANVAZ_URL,
                requestHeaders = mapOf(
                    "Authorization" to "Bearer $token",
                    "Accept" to "application/protobuf",
                    "Content-Type" to "application/protobuf",
                    BINARY_BODY_HEADER to BINARY_BODY_VALUE,
                ),
                body = Base64.encode(requestBytes),
            )
            if (response.statusCode !in 200..299) null
            else response.body?.let { parseCanvasUrl(Base64.decode(it)) }
        }.getOrNull()
        canvasMutex.withLock { canvasCache[trackId] = url }
        return url
    }

    // --- Connect: dealer socket -------------------------------------------------------------

    /** Opens the dealer socket if needed and returns its connection id. */
    private suspend fun connection(): String = connectionLock.withLock {
        connectionId?.takeIf { dealerJob?.isActive == true }?.let { return@withLock it }
        dealerJob?.cancel()
        val tok = token()
        val ready = CompletableDeferred<String>()
        dealerJob = scope.launch {
            try {
                webSocket.connect("wss://dealer.spotify.com/?access_token=$tok")
                val ping = launch {
                    while (isActive) {
                        delay(30_000)
                        runCatching { webSocket.send("{\"type\":\"ping\"}") }
                    }
                }
                try {
                    webSocket.events().collect { event ->
                        when (event) {
                            is WebSocketEvent.Text -> {
                                val message = runCatching { json.parseToJsonElement(event.text).jsonObject }.getOrNull()
                                    ?: return@collect
                                val id = (message["headers"] as? JsonObject)?.string("Spotify-Connection-Id")
                                if (id != null && !ready.isCompleted) {
                                    ready.complete(id)
                                } else if (message.string("uri")?.contains("connect-state") == true) {
                                    _changes.tryEmit(Unit)
                                }
                            }

                            is WebSocketEvent.Closed, is WebSocketEvent.Failed -> {
                                if (!ready.isCompleted) {
                                    ready.completeExceptionally(DeviceControlException(0, "dealer closed"))
                                }
                                connectionId = null
                            }
                        }
                    }
                } finally {
                    ping.cancel()
                }
            } catch (e: Exception) {
                if (!ready.isCompleted) ready.completeExceptionally(e)
            }
        }
        val id = withTimeout(12_000) { ready.await() }
        connectionId = id
        runCatching { registerController(id) }
        id
    }

    private suspend fun registerController(connection: String) {
        httpClient.request(
            method = HttpMethod.Post,
            url = "$SPCLIENT/track-playback/v1/devices",
            requestHeaders = authHeaders("Content-Type" to "application/json"),
            body = buildJsonObject {
                putJsonObject("device") {
                    put("brand", "spotify")
                    putJsonObject("capabilities") {
                        put("change_volume", true)
                        put("enable_play_token", true)
                        put("supports_file_media_type", true)
                        put("play_token_lost_behavior", "pause")
                        put("disable_connect", true)
                        put("audio_podcasts", true)
                        put("video_playback", false)
                    }
                    put("device_id", controllerId)
                    put("device_type", "computer")
                    putJsonObject("metadata") {}
                    put("model", "web_player")
                    put("name", "Prisma")
                    put("platform_identifier", "web_player linux undefined;chrome 131.0.0.0;desktop")
                    put("is_group", false)
                }
                put("outro_endcontent_snooping", false)
                put("connection_id", connection)
                put("client_version", "harmony:4.43.2-a61ecaf5")
                put("volume", 65535)
            }.toString(),
        )
    }

    /** Asks connect-state for the account's devices and what is playing; fills the caches. */
    private suspend fun refresh() {
        val connection = connection()
        val response = httpClient.request(
            method = HttpMethod.Put,
            url = "$SPCLIENT/connect-state/v1/devices/hobs_$controllerId",
            requestHeaders = authHeaders(
                "X-Spotify-Connection-Id" to connection,
                "Content-Type" to "application/json",
            ),
            body = buildJsonObject {
                put("member_type", "CONNECT_STATE")
                putJsonObject("device") {
                    putJsonObject("device_info") {
                        putJsonObject("capabilities") {
                            put("can_be_player", false)
                            put("hidden", true)
                            put("needs_full_player_state", true)
                        }
                    }
                }
            }.toString(),
        )
        if (response.statusCode !in 200..299) {
            dealerJob?.cancel()
            connectionId = null
            throw DeviceControlException(response.statusCode, response.body.orEmpty())
        }
        val cluster = json.parseToJsonElement(response.body.orEmpty()).jsonObject
        val active = cluster.string("active_device_id")?.takeIf { it.isNotBlank() }
        activeDeviceId = active

        devicesCache = (cluster["devices"] as? JsonObject).orEmpty().mapNotNull { (id, value) ->
            val device = value as? JsonObject ?: return@mapNotNull null
            if (id == controllerId || device.boolean("can_play") == false) return@mapNotNull null
            RemoteDevice(
                id = id,
                name = device.string("name").orEmpty(),
                type = device.string("device_type").orEmpty().lowercase().replaceFirstChar { it.uppercase() },
                isActive = id == active,
                volumePercent = device.long("volume")?.let { (it * 100 / 65535).toInt() },
            )
        }.sortedByDescending { it.isActive }

        val state = cluster["player_state"] as? JsonObject
        val track = state?.get("track") as? JsonObject
        val trackUri = track?.string("uri")
        playbackCache = if (state == null || active == null || trackUri.isNullOrBlank()) null else {
            val metadata = track?.get("metadata") as? JsonObject
            val playing = state.boolean("is_playing") == true && state.boolean("is_paused") != true
            val basePosition = state.long("position_as_of_timestamp") ?: 0L
            val since = state.long("timestamp")
            val serverNow = cluster.long("server_timestamp_ms") ?: Clock.System.now().toEpochMilliseconds()
            val duration = state.long("duration") ?: 0L
            val position = if (playing && since != null) basePosition + (serverNow - since) else basePosition
            val image = (metadata?.string("image_large_url") ?: metadata?.string("image_url"))
                ?.substringAfterLast(':')?.takeIf { it.isNotBlank() }
            RemotePlaybackState(
                device = devicesCache.firstOrNull { it.id == active },
                isPlaying = playing,
                positionMs = if (duration > 0) position.coerceIn(0L, duration) else position.coerceAtLeast(0L),
                item = RemotePlaybackItem(
                    id = trackUri.substringAfterLast(':'),
                    uri = trackUri,
                    title = metadata?.string("title").orEmpty(),
                    durationMs = duration,
                    artistNames = listOfNotNull(metadata?.string("artist_name")),
                    artworkUrl = image?.let { "https://i.scdn.co/image/$it" },
                ),
            )
        }
    }

    override suspend fun devices(): List<RemoteDevice> {
        refresh()
        return devicesCache
    }

    override suspend fun currentPlayback(): RemotePlaybackState? = playbackCache

    private suspend fun command(target: String, build: kotlinx.serialization.json.JsonObjectBuilder.() -> Unit) {
        connection()
        val response = httpClient.request(
            method = HttpMethod.Post,
            url = "$SPCLIENT/connect-state/v1/player/command/from/$controllerId/to/$target",
            requestHeaders = authHeaders("Content-Type" to "application/json"),
            body = buildJsonObject {
                putJsonObject("command") {
                    build()
                    putJsonObject("logging_params") { put("command_id", randomHex(32)) }
                }
            }.toString(),
        )
        checkStatus(response.statusCode, response.body)
    }

    private fun activeDevice(): String = activeDeviceId ?: throw DeviceControlException(404, "no active device")

    override suspend fun playOn(deviceId: String, trackUri: String?, positionMs: Long) {
        if (trackUri == null) return transfer(deviceId)
        command(deviceId) {
            putJsonObject("context") {
                put("uri", trackUri)
                put("url", "context://$trackUri")
                putJsonObject("metadata") {}
            }
            putJsonObject("play_origin") {
                put("feature_identifier", "harmony")
                put("feature_version", "4.43.2-a61ecaf5")
            }
            putJsonObject("options") {
                put("license", "on-demand")
                putJsonObject("skip_to") { put("track_uri", trackUri) }
                put("seek_to", positionMs.coerceAtLeast(0L))
                putJsonObject("player_options_override") {}
            }
            put("endpoint", "play")
        }
        activeDeviceId = deviceId
    }

    override suspend fun playTracks(deviceId: String, trackUris: List<String>, startIndex: Int, positionMs: Long) {
        val index = startIndex.coerceIn(0, (trackUris.size - 1).coerceAtLeast(0))
        command(deviceId) {
            putJsonObject("context") {
                put("uri", "spotify:internal:prisma")
                put("url", "context://spotify:internal:prisma")
                putJsonObject("metadata") {}
                putJsonArray("pages") {
                    addJsonObject {
                        putJsonArray("tracks") {
                            trackUris.take(MAX_LIST).forEach { uri -> addJsonObject { put("uri", uri) } }
                        }
                    }
                }
            }
            putJsonObject("play_origin") {
                put("feature_identifier", "harmony")
                put("feature_version", "4.43.2-a61ecaf5")
            }
            putJsonObject("options") {
                put("license", "on-demand")
                putJsonObject("skip_to") {
                    put("track_index", index)
                    put("track_uri", trackUris[index])
                }
                put("seek_to", positionMs.coerceAtLeast(0L))
                putJsonObject("player_options_override") {}
            }
            put("endpoint", "play")
        }
        activeDeviceId = deviceId
    }

    override suspend fun transfer(deviceId: String, play: Boolean) {
        connection()
        val response = httpClient.request(
            method = HttpMethod.Post,
            url = "$SPCLIENT/connect-state/v1/connect/transfer/from/$controllerId/to/$deviceId",
            requestHeaders = authHeaders("Content-Type" to "application/json"),
            body = buildJsonObject {
                putJsonObject("transfer_options") { put("restore_paused", if (play) "resume" else "restore") }
            }.toString(),
        )
        checkStatus(response.statusCode, response.body)
        activeDeviceId = deviceId
    }

    override suspend fun resume() = command(activeDevice()) { put("endpoint", "resume") }
    override suspend fun pause() = command(activeDevice()) { put("endpoint", "pause") }
    override suspend fun next() = command(activeDevice()) { put("endpoint", "skip_next") }
    override suspend fun previous() = command(activeDevice()) { put("endpoint", "skip_prev") }

    override suspend fun setVolume(percent: Int) {
        val target = activeDevice()
        connection()
        val response = httpClient.request(
            method = HttpMethod.Put,
            url = "$SPCLIENT/connect-state/v1/connect/volume/from/$controllerId/to/$target",
            requestHeaders = authHeaders("Content-Type" to "application/json"),
            body = buildJsonObject { put("volume", percent.coerceIn(0, 100) * 65535 / 100) }.toString(),
        )
        checkStatus(response.statusCode, response.body)
    }

    override suspend fun seek(positionMs: Long) = command(activeDevice()) {
        put("value", positionMs.coerceAtLeast(0L))
        put("endpoint", "seek_to")
    }

    override fun disconnect() {
        dealerJob?.cancel()
        dealerJob = null
        connectionId = null
        runCatching { scope.launch { webSocket.close() } }
    }

    private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull
    private fun JsonObject.boolean(key: String): Boolean? = (this[key] as? JsonPrimitive)?.booleanOrNull
    private fun JsonObject.long(key: String): Long? =
        (this[key] as? JsonPrimitive)?.let { it.longOrNull ?: it.contentOrNull?.toLongOrNull() }

    private fun randomHex(length: Int): String {
        val chars = "0123456789abcdef"
        return buildString { repeat(length) { append(chars[Random.nextInt(chars.length)]) } }
    }

    private companion object {
        val SPOTIFY_ID = Regex("^[0-9A-Za-z]{22}$")

        // EntityCanvazRequest { repeated Entity entities = 1; }  Entity { string entity_uri = 1; }
        fun requestBody(uri: String): ByteArray {
            val entity = lengthDelimited(1, uri.encodeToByteArray())
            return lengthDelimited(1, entity)
        }

        fun lengthDelimited(field: Int, bytes: ByteArray): ByteArray =
            byteArrayOf((field shl 3 or 2).toByte()) + varint(bytes.size.toLong()) + bytes

        fun varint(value: Long): ByteArray {
            val out = mutableListOf<Byte>()
            var v = value
            while (true) {
                if (v and 0x7FL.inv() == 0L) { out += v.toByte(); return out.toByteArray() }
                out += ((v and 0x7F) or 0x80).toByte()
                v = v ushr 7
            }
        }

        // EntityCanvazResponse { repeated Canvaz canvases = 1; }  Canvaz { string url = 2; Type type = 4; }
        fun parseCanvasUrl(bytes: ByteArray): String? {
            for (canvas in readFields(bytes).filter { it.first == 1 }.mapNotNull { it.second as? ByteArray }) {
                val fields = readFields(canvas)
                val url = (fields.firstOrNull { it.first == 2 }?.second as? ByteArray)?.decodeToString()
                val type = fields.firstOrNull { it.first == 4 }?.second as? Long ?: 0L
                if (url != null && type != 0L && url.endsWith(".mp4")) return url
            }
            return null
        }

        /** Minimal protobuf reader: (field number, Long for varints | ByteArray for bytes). */
        fun readFields(bytes: ByteArray): List<Pair<Int, Any>> {
            val out = mutableListOf<Pair<Int, Any>>()
            var i = 0
            fun readVarint(): Long {
                var shift = 0; var result = 0L
                while (i < bytes.size) {
                    val b = bytes[i++].toInt() and 0xFF
                    result = result or ((b and 0x7F).toLong() shl shift)
                    if (b and 0x80 == 0) break
                    shift += 7
                }
                return result
            }
            while (i < bytes.size) {
                val key = readVarint()
                val field = (key ushr 3).toInt()
                when ((key and 7).toInt()) {
                    0 -> out += field to readVarint()
                    1 -> i += 8
                    2 -> {
                        val len = readVarint().toInt()
                        if (len < 0 || i + len > bytes.size) return out
                        out += field to bytes.copyOfRange(i, i + len)
                        i += len
                    }
                    5 -> i += 4
                    else -> return out
                }
            }
            return out
        }
    }
}
