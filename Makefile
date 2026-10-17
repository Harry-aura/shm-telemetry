SHELL := /bin/bash
CC ?= gcc

CFLAGS_COMMON := -std=gnu11 -D_GNU_SOURCE -Wall -Wextra -Werror -Wshadow -Wformat=2 -pthread -Inative -MMD -MP
CFLAGS_REL := -O2
CFLAGS_DBG := -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=undefined
LDFLAGS := -pthread -lrt

SRCS := native/shm_common.c native/producer.c native/consumer.c native/bridge_daemon.c native/stress_test.c
NAMES := shm_common producer consumer bridge_daemon stress_test
ALL_BINS := producer consumer bridge_daemon stress_test

REL_DIR := build
DBG_DIR := build-debug

REL_BINS := $(addprefix $(REL_DIR)/,$(ALL_BINS))
DBG_BINS := $(addprefix $(DBG_DIR)/,$(ALL_BINS))

define binrule
$(1)/$(2): $(1)/$(2).o $(1)/shm_common.o
	$$(CC) $$(CFLAGS_COMMON) $(3) $$^ -o $$@ $$(LDFLAGS)
endef

.PHONY: all debug stress stress-debug run-demo demo-check web-install web-build clean check help

all: $(REL_BINS)

debug: $(DBG_BINS)

$(REL_DIR)/%.o: native/%.c native/shm_common.h | $(REL_DIR)
	$(CC) $(CFLAGS_COMMON) $(CFLAGS_REL) -c $< -o $@

$(DBG_DIR)/%.o: native/%.c native/shm_common.h | $(DBG_DIR)
	$(CC) $(CFLAGS_COMMON) $(CFLAGS_DBG) -c $< -o $@

$(REL_DIR) $(DBG_DIR):
	mkdir -p $@

$(eval $(call binrule,$(REL_DIR),producer,$(CFLAGS_REL)))
$(eval $(call binrule,$(REL_DIR),consumer,$(CFLAGS_REL)))
$(eval $(call binrule,$(REL_DIR),bridge_daemon,$(CFLAGS_REL)))
$(eval $(call binrule,$(REL_DIR),stress_test,$(CFLAGS_REL)))
$(eval $(call binrule,$(DBG_DIR),producer,$(CFLAGS_DBG)))
$(eval $(call binrule,$(DBG_DIR),consumer,$(CFLAGS_DBG)))
$(eval $(call binrule,$(DBG_DIR),bridge_daemon,$(CFLAGS_DBG)))
$(eval $(call binrule,$(DBG_DIR),stress_test,$(CFLAGS_DBG)))

stress: all
	./$(REL_DIR)/stress_test

stress-debug: debug
	./$(DBG_DIR)/stress_test

run-demo: all
	@./cleanup.sh -f >/dev/null 2>&1 || true
	@set -m; \
	trap 'kill $$p1 $$p2 $$p3 2>/dev/null || true' INT TERM EXIT; \
	./$(REL_DIR)/producer 0 100 0 & p1=$$!; \
	./$(REL_DIR)/consumer 0 0 & p2=$$!; \
	./$(REL_DIR)/bridge_daemon 0 & p3=$$!; \
	echo "bridge socket: /tmp/shm_bridge_0.sock (Ctrl-C to stop)"; \
	wait

demo-check: all
	@./cleanup.sh -f >/dev/null 2>&1 || true
	@set -m; \
	trap 'kill $$p1 $$p2 $$p3 2>/dev/null || true' INT TERM EXIT; \
	./$(REL_DIR)/producer 0 0 4 >/dev/null 2>&1 & p1=$$!; \
	./$(REL_DIR)/consumer 0 4 >/dev/null 2>&1 & p2=$$!; \
	./$(REL_DIR)/bridge_daemon 0 >/dev/null 2>&1 & p3=$$!; \
	sleep 1; \
	node scripts/validate_bridge.js /tmp/shm_bridge_0.sock 30; \
	rc=$$?; \
	kill $$p1 $$p2 $$p3 2>/dev/null || true; \
	wait 2>/dev/null || true; \
	./cleanup.sh -f >/dev/null 2>&1 || true; \
	if [ $$rc -eq 0 ]; then echo "DEMO-CHECK PASS"; else echo "DEMO-CHECK FAIL"; fi; \
	exit $$rc

web-install:
	cd server && npm install
	cd client && npm install

web-build:
	cd server && npm run build
	cd client && npm run build

check:
	$(MAKE) clean
	$(MAKE) all
	$(MAKE) debug
	$(MAKE) stress
	$(MAKE) stress-debug
	./cleanup.sh -n
	$(MAKE) web-build

clean:
	rm -rf $(REL_DIR) $(DBG_DIR)

help:
	@echo "make all          build release binaries into build/"
	@echo "make debug        build sanitized binaries into build-debug/"
	@echo "make stress       run build/stress_test"
	@echo "make stress-debug run build-debug/stress_test (ASan+UBSan)"
	@echo "make run-demo     run producer+consumer+bridge in the foreground (Ctrl-C)"
	@echo "make demo-check   run the three binaries for ~4s and validate 30 bridge frames"
	@echo "make web-install  npm install in server/ and client/"
	@echo "make web-build    npm run build in server/ and client/"
	@echo "make check        run all gates in order"
	@echo "make clean        remove build/ and build-debug/"

-include $(REL_BINS:=.d)
-include $(DBG_BINS:=.d)
