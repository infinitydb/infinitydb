// MIT License
//
// Copyright (c) 2023 Roger L. Deran
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Instead of these, you can also write { _myAttribute : 5; }

class EntityClass {
	constructor(name) {
		this.name = name;
	}

	toString() {
		return this.name;
	}
}

class Attribute {
	constructor(name) {
		this.name = name;
	}

	toString() {
		return this.name;
	}
}

// Note we consider ArrayBuff to be UTF-8, not InfinityDB byte array!
// To get blobs, use action=get-blob, action=put-blob, action=execute-get-blob-query,
// or action=put-blob-query.
function qKey(o) {
	if (o === null || o === undefined) {
		throw new TypeError('keys must not be null');
	} else if (typeof o === 'boolean') {
		return o ? '_true' : '_false';
	} else if (typeof o === 'number') {
		// default is always double! You do not have to quote! You will forget this.
		return qDouble(o);
	} else if (typeof o === 'string') {
		return o.charAt(0) !== '_' ? o : '_' + o;
	} else if (o instanceof Date) {
		return '_' + o.toISOString();
	} else if (o instanceof EntityClass || o instanceof Attribute) {
		return '_' + o.toString();
	} else if (typeof o === 'object' && o.constructor === ArrayBuffer) {
		// Convert ArrayBuffer to a string representation (works in both contexts)
		const decoder = new TextDecoder('utf-8');
		const decodedString = decoder.decode(o);
		return decodedString.charAt(0) !== '_' ? decodedString : '_' + decodedString;
	} else {
		throw new TypeError('keys must be primitive or date');
	}
}

// Only needed for keys, or just use qKey() everywhere
function qDouble(n) {
	return '_' + toDoubleString(n);
}

function toDoubleString(n) {
	return Number.isInteger(n) ? n + '.0' : n.toString();
}

function qFloat(n) {
	return '_' + toFloatString(n);
}

function toFloatString(n) {
	return Number.isInteger(n) ? n + '.0f' : n.toString() + 'f';
}

function qLong(n) {
	return '_' + toLongString(n);
}

function toLongString(n) {
	return '' + Math.floor(n);
}

// Note when the JSON is parsed at the InfinityDB end, all numbers
// are considered double, so you have to use qLong(n) and qFloat(n)
// instead to provide context and generate '_5' or '_5.0f'.
function qValue(o) {
	if (o === null || o === undefined) {
		return null;
	} else if (typeof o === 'boolean' || typeof o === 'number') {
		return o;
	} else {
		return qKey(o);
	}
}

// Unquote an underscore-quoted value
// This works for keys or values
// Be careful with numbers: InfinityDB long and float become JS numbers
function uq(o) {
	if (typeof o !== 'string')
		return o;
	if (o === '') {
		return '';
	} else if (o.charAt(0) !== '_') {
		return o;
	} else if (o.startsWith('__')) {
		return o.slice(1);
	}
	// it is a string starting with a single _
	o = o.slice(1);
	if (o === 'null') {
		return null;
	} else if (o === 'true') {
		return true;
	} else if (o === 'false') {
		return false;
	} else if (!isNaN(Number(o))) {
		return parseFloat(o);
	} else if (o.endsWith('f') && !isNaN(Number(o.slice(0, -1)))) {
		return parseFloat(o.slice(0, -1));
	} else if (isValidIsoDate(o)) {
		return new Date(o);
	} else if (isValidEntityClass(o)) {
		return new EntityClass(o);
	} else if (isValidAttribute(o)) {
		return new Attribute(o);
	} else {
		throw new TypeError('cannot underscore-unquote ' + o);
	}
}

function isValidIsoDate(dateString) {
	const isoDateRegex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?)(Z|[-+]\d{2}:\d{2})?$/;
	return isoDateRegex.test(dateString);
}


function isValidEntityClass(s) {
	const metaRegex = /^[A-Z][a-zA-Z\d\._]*$/;
	return metaRegex.test(s);
}

function isValidAttribute(s) {
	const metaRegex = /^[a-z][a-zA-Z\d\._]*$/;
	return metaRegex.test(s);
}

function exampleCreateObject() {
	const o = 'anything';
	// any string at all
	const s = '_s';
	// classes start with UC, then any letters, digits, underscores or dots
	// These are really optional - you can just add an underscore except for dots
	const C = new EntityClass('C_a.b');
	// classes start with LC, then any letters, digits, underscores or dots
	const a = new Attribute('a_a.b');
	const n = 5;

	const object = {
		// Quote any type, but all numbers become double!
		[qKey(o)]: qValue(o),
		// This forces purely ISO dates
		[qKey(new Date())]: qValue(new Date()),
		['_2023-09-17T21:24:35.653Z']: qValue(new Date('2023-09-17T21:24:35.653Z')),
		['_2023-09-17T21:24:35.653-07:00']: qValue(new Date('2023-09-17T21:24:35.653-07:00')),

		// Use qLong() to add the '_' to force long, which is a 64-bit integer
		// on the InfinityDB side
		_5: '_5',
		[qLong(n)]: qLong(n),
		// Use qFloat() to add the '_' and 'f' to force float on the InfinityDB side
		_5f: '_5f',
		'_5.0f': '_5.0f',
		[qFloat(n)]: qFloat(n),
		// Use the default double for numbers.
		// You will forget that double is default, not long!!
		'_5.0': 5,
		'_5.1': 5.1,
		'_5.1': '_5.1',
		[qKey(n)]: n,
		// Here x is a string on the InfinityDB side, not an attribute or class
		'x': 'x',
		// here _C is a class without the '_' on the InfinityDB side
		'_C_.a': '_C_.a',
		// Here _a is an attribute without the '_' on the InfinityDB side
		'_a._a': '_a._a',
		// here __s is a string starting with single quote '_' on the InfinityDB side
		// We add an extra '_'
		'__some string': '__some string',
		// s is unknown string automatically underscore quoted if necessary
		[qKey(s)]: qValue(s),
		// Unknown class or attribute
		[qKey(C)]: qValue(C),
		[qKey(a)]: qValue(a),
		[qKey(true)]: true,
		[qKey(false)]: false
	};
	return object;
}

function exampleUnquoteObject(o) {
	for (const key in o) {
		const k = uq(key);
		const v = uq(o[key]);
		console.log(`Key: ${typeof k}:${k}, Value: ${typeof v}:${v}`);
	}
}

const axios = require('axios');
let https;
try {
	https = require('node:https');
} catch (err) {
	console.error('https support is disabled!');
}

function main() {

	// Demo the underscore-quoting functions

	const object = exampleCreateObject();
	console.log(JSON.stringify(object));
	exampleUnquoteObject(object);

	// Use the axios module

	try {
		axios.get('https://infinitydb.com:37411/infinitydb/data/demo/readonly/Documentation',
			{ auth: { username: 'testUser', password: 'db' } }
		).then(function(response) {
			console.log('From axios: ' + response.data.Basics._description[0]);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}

	// Use the https module	

	const options = {
		hostname: 'infinitydb.com',
		path: '/infinitydb/data/demo/readonly/Documentation',
		port: 37411,
		method: 'GET',
		headers: {
			'Authorization': 'Basic ' + Buffer.from('testUser:db').toString('base64')
		}
	};

	const req = https.request(options, (res) => {
		let data = '';

		res.on('data', (chunk) => {
			data += chunk;
		});
		res.on('end', () => {
			console.log('From https: ' + JSON.parse(data).Basics._description[0]);
		});
	});
	req.on('error', (error) => {
		console.error(error);
	});
	req.end();
}

main();
