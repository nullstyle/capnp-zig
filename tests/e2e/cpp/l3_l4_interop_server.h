#pragma once

#include <kj/async-io.h>

void runL3L4InteropServer(kj::ConnectionReceiver& listener, kj::WaitScope& waitScope);
