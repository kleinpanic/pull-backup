PREFIX   ?= /usr/local
SBINDIR  ?= $(PREFIX)/sbin
SYSDDIR  ?= /etc/systemd/system
CONFDIR  ?= /etc/pull-backup
INSTALL  ?= install

SCRIPT        := pull-backup
DEST_SCRIPT   := $(SBINDIR)/pull-backup

UNIT_SERVICE  := systemd/pull-backup.service
UNIT_TIMER    := systemd/pull-backup.timer
DEST_SERVICE  := $(SYSDDIR)/pull-backup.service
DEST_TIMER    := $(SYSDDIR)/pull-backup.timer

VENV      ?= .venv
VENV_PY   := $(VENV)/bin/python
VENV_PIP  := $(VENV)/bin/pip
VENV_STAMP := $(VENV)/.pytest.installed

.PHONY: all check install install-script install-units install-config reload enable start stop disable \
        uninstall uninstall-units uninstall-script reinstall test-run status logs \
        venv test clean-venv

all:
	@echo "Targets:"
	@echo "  install         - install script + systemd units, reload, enable timer"
	@echo "  install-config  - create $(CONFDIR) if missing (does NOT overwrite jobs.toml)"
	@echo "  start           - start the timer now"
	@echo "  test-run        - run one oneshot backup now (service)"
	@echo "  logs            - follow service logs"
	@echo "  status          - show timer/service status"
	@echo "  test            - create venv, install pytest, run tests"
	@echo "  clean-venv      - remove local venv"
	@echo "  uninstall       - remove script + units, disable timer"

check:
	@python3 -m py_compile $(SCRIPT)

# ----------------------------
# Venv + tests
# ----------------------------

venv: $(VENV_STAMP)

$(VENV_STAMP):
	@set -e; \
	if [ ! -x "$(VENV_PY)" ]; then \
		echo "[venv] creating $(VENV)"; \
		python3 -m venv "$(VENV)"; \
	fi; \
	echo "[venv] upgrading pip"; \
	"$(VENV_PIP)" -q install --upgrade pip setuptools wheel; \
	echo "[venv] installing pytest"; \
	"$(VENV_PIP)" -q install pytest; \
	touch "$(VENV_STAMP)"

test: venv
	@$(VENV_PY) -m pytest -q

clean-venv:
	rm -rf "$(VENV)"

# ----------------------------
# Install / systemd
# ----------------------------

install: install-script install-units install-config reload enable
	@echo "Installed script + units. Timer enabled."

install-script: check
	$(INSTALL) -d -m 0755 $(SBINDIR)
	$(INSTALL) -m 0755 $(SCRIPT) $(DEST_SCRIPT)
	@echo "Installed: $(DEST_SCRIPT)"

install-units:
	$(INSTALL) -m 0644 $(UNIT_SERVICE) $(DEST_SERVICE)
	$(INSTALL) -m 0644 $(UNIT_TIMER) $(DEST_TIMER)
	@echo "Installed: $(DEST_SERVICE)"
	@echo "Installed: $(DEST_TIMER)"

install-config:
	$(INSTALL) -d -m 0755 $(CONFDIR)
	@test -f $(CONFDIR)/jobs.toml || echo "# create $(CONFDIR)/jobs.toml" > /dev/null
	@echo "Ensured: $(CONFDIR)/ (jobs.toml not overwritten)"

reload:
	systemctl daemon-reload

enable:
	systemctl enable pull-backup.timer

start:
	systemctl start pull-backup.timer

stop:
	-systemctl stop pull-backup.service
	-systemctl stop pull-backup.timer

disable:
	-systemctl disable pull-backup.timer

status:
	systemctl status pull-backup.timer --no-pager || true
	@echo "----"
	systemctl status pull-backup.service --no-pager || true

logs:
	journalctl -u pull-backup.service -f -o cat

test-run:
	systemctl start pull-backup.service
	@echo "Started pull-backup.service. Follow logs with: make logs"
	@echo "Or check status with: make status"

uninstall: stop disable uninstall-units uninstall-script reload
	@echo "Uninstalled script + units."

uninstall-units:
	rm -f $(DEST_SERVICE) $(DEST_TIMER)
	@echo "Removed: $(DEST_SERVICE)"
	@echo "Removed: $(DEST_TIMER)"

uninstall-script:
	rm -f $(DEST_SCRIPT)
	@echo "Removed: $(DEST_SCRIPT)"

reinstall: uninstall install

