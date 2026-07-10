const { createRequire } = require("module");

const rootRequire = createRequire(`${process.cwd()}/`);
const { Float64, Int32, Utf8, makeData } = rootRequire("apache-arrow");

function makeArrowJsInt32Data() {
	return makeData({
		type: new Int32(),
		data: new Int32Array([1, 2, 3]),
	});
}

function makeArrowJsFloat64Data() {
	return makeData({
		type: new Float64(),
		data: new Float64Array([1.5, -2.0]),
	});
}

function makeArrowJsUtf8Data() {
	return makeData({
		type: new Utf8(),
		valueOffsets: new Int32Array([0, 2, 5]),
		data: new Uint8Array([97, 98, 99, 100, 101]),
	});
}

module.exports = {
	makeArrowJsInt32Data,
	makeArrowJsFloat64Data,
	makeArrowJsUtf8Data,
};
