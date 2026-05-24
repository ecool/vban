#ifndef __PIPEWIRE_BACKEND_H__
#define __PIPEWIRE_BACKEND_H__

#include "audio_backend.h"

#define PIPEWIRE_BACKEND_NAME   "pipewire"
int pipewire_backend_init(audio_backend_handle_t* handle);

#endif /*__PIPEWIRE_BACKEND_H__*/
