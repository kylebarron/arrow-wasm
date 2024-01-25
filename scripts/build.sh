#! /usr/bin/env bash
rm -rf tmp_build pkg
mkdir -p tmp_build

if [ "$ENV" == "DEV" ]; then
  BUILD="--dev"
  FLAGS="--features debug"
else
  BUILD="--release"
  FLAGS=""
fi

# Build node version into tmp_build/node
echo "Building node"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/node \
  --target nodejs \
  --features all \
  $FLAGS &
[ -n "$CI" ] && wait;

# Build web version into tmp_build/esm
echo "Building esm"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/esm \
  --target web \
  --features all \
  $FLAGS &
[ -n "$CI" ] && wait;

# Build bundler version into tmp_build/bundler
echo "Building bundler"
wasm-pack build \
  $BUILD \
  --out-dir tmp_build/bundler \
  --target bundler \
  --features all \
  $FLAGS &
wait

# Copy files into pkg/
mkdir -p pkg/{node,esm,bundler}

cp tmp_build/bundler/arrow* pkg/bundler/
cp tmp_build/esm/arrow* pkg/esm
cp tmp_build/node/arrow* pkg/node

cp tmp_build/bundler/{package.json,LICENSE_APACHE,LICENSE_MIT,README.md} pkg/

# Create minimal package.json in esm/ folder with type: module
echo '{"type": "module"}' > pkg/esm/package.json

# Create an esm2/ directory without import.meta.url in the JS ESM wrapper
cp -r pkg/esm pkg/esm2
sed '/import.meta.url/ s|input|// input|' pkg/esm2/arrow_wasm.js > pkg/esm2/arrow_wasm_new.js
mv pkg/esm2/arrow_wasm_new.js pkg/esm2/arrow_wasm.js

# Update files array in package.json using JQ
jq '.files = ["*"] | .module="bundler/arrow_wasm.js" | .types="bundler/arrow_wasm.d.ts"' pkg/package.json > pkg/package.json.tmp

# Overwrite existing package.json file
mv pkg/package.json.tmp pkg/package.json

rm -rf tmp_build
