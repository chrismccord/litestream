SQLITE_SRC = ../../src

CFLAGS = -shared -fPIC -I$(SQLITE_SRC) -Wall -Wextra -O2 -pthread
TARGET = litestream_sync.so
SRC = litestream_sync.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGET)
