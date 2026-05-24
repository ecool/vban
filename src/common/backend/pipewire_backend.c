#include "pipewire_backend.h"
#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include "common/logger.h"

struct pipewire_backend_t
{
    struct audio_backend_t parent;
    struct pw_thread_loop* loop;
    struct pw_stream* stream;
    enum audio_direction direction;
    size_t frame_size;

    /* ring buffer for transferring data between PipeWire thread and caller */
    char* ring_buffer;
    size_t ring_size;
    size_t ring_read_pos;
    size_t ring_write_pos;
    size_t ring_filled;
};

static int pipewire_open(audio_backend_handle_t handle, const char* device_name, const char* application_name,
                         enum audio_direction direction, size_t buffer_size, const struct stream_config_t* config);
static int pipewire_close(audio_backend_handle_t handle);
static int pipewire_write(audio_backend_handle_t handle, const char* data, size_t size);
static int pipewire_read(audio_backend_handle_t handle, char* data, size_t size);

static enum spa_audio_format vban_to_pipewire_format(enum VBanBitResolution bit_resolution)
{
    switch (bit_resolution)
    {
    case VBAN_BITFMT_8_INT:
        return SPA_AUDIO_FORMAT_U8;

    case VBAN_BITFMT_16_INT:
        return SPA_AUDIO_FORMAT_S16_LE;

    case VBAN_BITFMT_24_INT:
        return SPA_AUDIO_FORMAT_S24_LE;

    case VBAN_BITFMT_32_INT:
        return SPA_AUDIO_FORMAT_S32_LE;

    case VBAN_BITFMT_32_FLOAT:
        return SPA_AUDIO_FORMAT_F32_LE;

    case VBAN_BITFMT_64_FLOAT:
        return SPA_AUDIO_FORMAT_F64_LE;

    default:
        return SPA_AUDIO_FORMAT_UNKNOWN;
    }
}

/* --- ring buffer helpers (called under thread loop lock) --- */

static size_t ring_avail_read(struct pipewire_backend_t* pw)
{
    return pw->ring_filled;
}

static size_t ring_avail_write(struct pipewire_backend_t* pw)
{
    return pw->ring_size - pw->ring_filled;
}

static size_t ring_write(struct pipewire_backend_t* pw, const char* src, size_t len)
{
    size_t avail = ring_avail_write(pw);
    if (len > avail)
        len = avail;

    for (size_t i = 0; i < len; i++)
    {
        pw->ring_buffer[pw->ring_write_pos] = src[i];
        pw->ring_write_pos = (pw->ring_write_pos + 1) % pw->ring_size;
    }
    pw->ring_filled += len;
    return len;
}

static size_t ring_read(struct pipewire_backend_t* pw, char* dst, size_t len)
{
    size_t avail = ring_avail_read(pw);
    if (len > avail)
        len = avail;

    for (size_t i = 0; i < len; i++)
    {
        dst[i] = pw->ring_buffer[pw->ring_read_pos];
        pw->ring_read_pos = (pw->ring_read_pos + 1) % pw->ring_size;
    }
    pw->ring_filled -= len;
    return len;
}

/* --- PipeWire stream callbacks --- */

static void on_process_playback(void* userdata)
{
    auto pw = userdata;
    struct pw_buffer* b;
    struct spa_buffer* buf;
    char* dst;
    size_t n_bytes;
    size_t copied;

    b = pw_stream_dequeue_buffer(pw->stream);
    if (b == NULL)
    {
        logger_log(LOG_WARNING, "%s: out of buffers", __func__);
        return;
    }

    buf = b->buffer;
    dst = buf->datas[0].data;
    if (dst == NULL)
        goto done;

    n_bytes = buf->datas[0].maxsize;
    if (b->requested != 0)
    {
        size_t req = b->requested * pw->frame_size;
        if (req < n_bytes)
            n_bytes = req;
    }

    copied = ring_read(pw, dst, n_bytes);

    /* silence any remaining portion */
    if (copied < n_bytes)
        memset(dst + copied, 0, n_bytes - copied);

    buf->datas[0].chunk->offset = 0;
    buf->datas[0].chunk->stride = (int32_t)pw->frame_size;
    buf->datas[0].chunk->size = (uint32_t)n_bytes;

done:
    pw_stream_queue_buffer(pw->stream, b);

    /* wake up the writer if it was waiting for space */
    pw_thread_loop_signal(pw->loop, false);
}

static void on_process_capture(void* userdata)
{
    auto pw = userdata;
    struct pw_buffer* b;
    struct spa_buffer* buf;
    const char* src;
    size_t n_bytes;

    b = pw_stream_dequeue_buffer(pw->stream);
    if (b == NULL)
    {
        logger_log(LOG_WARNING, "%s: out of buffers", __func__);
        return;
    }

    buf = b->buffer;
    src = buf->datas[0].data;
    if (src == NULL)
        goto done;

    n_bytes = buf->datas[0].chunk->size;
    ring_write(pw, src, n_bytes);

done:
    pw_stream_queue_buffer(pw->stream, b);

    /* wake up the reader if it was waiting for data */
    pw_thread_loop_signal(pw->loop, false);
}

static const struct pw_stream_events playback_stream_events = {
    PW_VERSION_STREAM_EVENTS,
    .process = on_process_playback,
};

static const struct pw_stream_events capture_stream_events = {
    PW_VERSION_STREAM_EVENTS,
    .process = on_process_capture,
};

/* --- backend interface implementation --- */

int pipewire_backend_init(audio_backend_handle_t* handle)
{
    struct pipewire_backend_t* pw_backend = 0;

    if (handle == 0)
    {
        logger_log(LOG_FATAL, "%s: null handle pointer", __func__);
        return -EINVAL;
    }

    pw_backend = calloc(1, sizeof(struct pipewire_backend_t));
    if (pw_backend == 0)
    {
        logger_log(LOG_FATAL, "%s: could not allocate memory", __func__);
        return -ENOMEM;
    }

    pw_backend->parent.open = pipewire_open;
    pw_backend->parent.close = pipewire_close;
    pw_backend->parent.write = pipewire_write;
    pw_backend->parent.read = pipewire_read;

    *handle = (audio_backend_handle_t)pw_backend;

    return 0;
}

int pipewire_open(audio_backend_handle_t handle, const char* device_name, const char* application_name,
                  enum audio_direction direction, size_t buffer_size, const struct stream_config_t* config)
{
    const auto pw = (struct pipewire_backend_t*)handle;
    const struct pw_stream_events* events;
    enum pw_direction pw_dir;
    enum pw_stream_flags flags;
    uint8_t pod_buffer[1024];
    struct spa_pod_builder builder;
    const struct spa_pod* params[1];
    struct spa_audio_info_raw info;
    int ret;

    if (handle == 0)
    {
        logger_log(LOG_FATAL, "%s: handle pointer is null", __func__);
        return -EINVAL;
    }

    pw->direction = direction;
    pw->frame_size = VBanBitResolutionSize[config->bit_fmt] * config->nb_channels;

    /* allocate ring buffer (4x the requested buffer size for headroom) */
    pw->ring_size = buffer_size * 4;
    pw->ring_buffer = calloc(1, pw->ring_size);
    if (pw->ring_buffer == 0)
    {
        logger_log(LOG_FATAL, "%s: could not allocate ring buffer", __func__);
        return -ENOMEM;
    }
    pw->ring_read_pos = 0;
    pw->ring_write_pos = 0;
    pw->ring_filled = 0;

    /* initialize PipeWire */
    pw_init(NULL, NULL);

    pw->loop = pw_thread_loop_new("vban-pipewire", NULL);
    if (pw->loop == NULL)
    {
        logger_log(LOG_FATAL, "%s: could not create thread loop", __func__);
        free(pw->ring_buffer);
        pw->ring_buffer = 0;
        return -ENOMEM;
    }

    if (direction == AUDIO_OUT)
    {
        events = &playback_stream_events;
        pw_dir = PW_DIRECTION_OUTPUT;
    }
    else
    {
        events = &capture_stream_events;
        pw_dir = PW_DIRECTION_INPUT;
    }

    pw->stream = pw_stream_new_simple(
        pw_thread_loop_get_loop(pw->loop),
        application_name,
        pw_properties_new(
            PW_KEY_MEDIA_TYPE, "Audio",
            PW_KEY_MEDIA_CATEGORY, (direction == AUDIO_OUT) ? "Playback" : "Capture",
            PW_KEY_MEDIA_ROLE, "Communication",
            (device_name[0] != '\0') ? PW_KEY_TARGET_OBJECT : NULL,
            (device_name[0] != '\0') ? device_name : NULL,
            NULL),
        events,
        pw);

    if (pw->stream == NULL)
    {
        logger_log(LOG_FATAL, "%s: could not create stream", __func__);
        pw_thread_loop_destroy(pw->loop);
        pw->loop = NULL;
        free(pw->ring_buffer);
        pw->ring_buffer = 0;
        return -ENOMEM;
    }

    /* build the audio format pod */
    memset(&info, 0, sizeof(info));
    info.format = vban_to_pipewire_format(config->bit_fmt);
    info.rate = config->sample_rate;
    info.channels = config->nb_channels;

    spa_pod_builder_init(&builder, pod_buffer, sizeof(pod_buffer));
    params[0] = spa_format_audio_raw_build(&builder, SPA_PARAM_EnumFormat, &info);

    flags = PW_STREAM_FLAG_AUTOCONNECT | PW_STREAM_FLAG_MAP_BUFFERS | PW_STREAM_FLAG_RT_PROCESS;

    ret = pw_stream_connect(pw->stream, pw_dir, PW_ID_ANY, flags, params, 1);
    if (ret < 0)
    {
        logger_log(LOG_FATAL, "%s: could not connect stream: %s", __func__, spa_strerror(ret));
        pw_stream_destroy(pw->stream);
        pw->stream = NULL;
        pw_thread_loop_destroy(pw->loop);
        pw->loop = NULL;
        free(pw->ring_buffer);
        pw->ring_buffer = 0;
        return ret;
    }

    pw_thread_loop_start(pw->loop);

    return 0;
}

int pipewire_close(audio_backend_handle_t handle)
{
    const auto pw = (struct pipewire_backend_t*)handle;

    if (handle == 0)
    {
        logger_log(LOG_FATAL, "%s: handle pointer is null", __func__);
        return -EINVAL;
    }

    if (pw->loop != NULL)
    {
        pw_thread_loop_stop(pw->loop);
    }

    if (pw->stream != NULL)
    {
        pw_stream_destroy(pw->stream);
        pw->stream = NULL;
    }

    if (pw->loop != NULL)
    {
        pw_thread_loop_destroy(pw->loop);
        pw->loop = NULL;
    }

    if (pw->ring_buffer != NULL)
    {
        free(pw->ring_buffer);
        pw->ring_buffer = NULL;
    }

    pw_deinit();

    return 0;
}

int pipewire_write(audio_backend_handle_t handle, const char* data, size_t size)
{
    const auto pw = (struct pipewire_backend_t*)handle;
    size_t total_written = 0;

    if ((handle == 0) || (data == 0))
    {
        logger_log(LOG_ERROR, "%s: handle or data pointer is null", __func__);
        return -EINVAL;
    }

    if (pw->stream == 0)
    {
        logger_log(LOG_ERROR, "%s: device not open", __func__);
        return -ENODEV;
    }

    pw_thread_loop_lock(pw->loop);

    while (total_written < size)
    {
        size_t written = ring_write(pw, data + total_written, size - total_written);
        total_written += written;

        if (total_written < size)
        {
            /* ring buffer full – wait for PipeWire to drain some */
            pw_thread_loop_wait(pw->loop);
        }
    }

    pw_thread_loop_unlock(pw->loop);

    return (int)total_written;
}

int pipewire_read(audio_backend_handle_t handle, char* data, size_t size)
{
    const auto pw = (struct pipewire_backend_t*)handle;
    size_t total_read = 0;

    if ((handle == 0) || (data == 0))
    {
        logger_log(LOG_ERROR, "%s: handle or data pointer is null", __func__);
        return -EINVAL;
    }

    if (pw->stream == 0)
    {
        logger_log(LOG_ERROR, "%s: device not open", __func__);
        return -ENODEV;
    }

    pw_thread_loop_lock(pw->loop);

    while (total_read < size)
    {
        size_t nread = ring_read(pw, data + total_read, size - total_read);
        total_read += nread;

        if (total_read < size)
        {
            /* ring buffer empty – wait for PipeWire to supply more data */
            pw_thread_loop_wait(pw->loop);
        }
    }

    pw_thread_loop_unlock(pw->loop);

    return (int)total_read;
}
