#!/usr/bin/env bash
# Sourced by the other scripts to put every toolchain used by this repo on
# PATH: Go (tarball install, not apt), Rust (rustup), and JDK 21 for the
# fanout-worker (the system default `java` may point at an older JDK).
export PATH="$HOME/go-toolchain/go/bin:$PATH"
export GOPATH="$HOME/go"

if [ -f "$HOME/.cargo/env" ]; then
  source "$HOME/.cargo/env"
fi

if [ -d /usr/lib/jvm/jdk-21-oracle-x64 ]; then
  export JAVA_HOME=/usr/lib/jvm/jdk-21-oracle-x64
elif [ -d /usr/lib/jvm/java-21-openjdk-amd64 ]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
fi
export PATH="$JAVA_HOME/bin:$PATH"
