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

// Here are the classes that represent the 12 InfinityDB data types that
// are not primitive..

// Instead of these, you can also write { _MyClass : 5; }, but that
// will not normally be necessary because these will be hidden
// since parsing will produce lists. These are for including within
// Items in order to indicate an offset in a list. These will still
// happen in a URL path, though.

class EntityClass {
	constructor(name) {
		this.name = name;
	}

	static isValidEntityClass(s) {
		const metaRegex = /^[A-Z][a-zA-Z\d\._]*$/;
		return metaRegex.test(s);
	}
	
	toString() {
		return this.name;
	}
}

// Instead of these, you can also write { _myAttribute : 5; }
class Attribute {
	constructor(name) {
		this.name = name;
	}
	
	static isValidAttribute(s) {
		const metaRegex = /^[a-z][a-zA-Z\d\._]*$/;
		return metaRegex.test(s);
	}

	toString() {
		return this.name;
	}
}

// Instead of this you can also write '_[n]' like '_[55]'

class Index {
	constructor(index) {
		if (index == null) {
			index = 0;
		} else if (typeof index !== 'number') {
   			throw new Error('Constructing an Index: expected a number but was ' + index);
		}
		this.index = index;
	}
	parse(s) {
		if (typeof s === 'string') {
			if (s.startsWith('Index(') && s.endsWith(')')) {
				s = s.slice(6, -1);
			} else if (s.startsWith('[') && s.endsWith(']')) {
				s = s.slice(1, -1);
			} else {
			   	throw new Error('expected string to parse as Index(n) or [n] but was ' + s);
			}
		} else {
			 throw new Error('expected string to parse an Index but was ' + s);
		}
		this.index = parseInt(s);
		if (isNaN(this.index) || !Number.isInteger(this.index)) {
			throw new Error('expected "Index(n) but n is not an integer: it was "' + this.index);
		}
		return this;
	}
	
	getIndex() {
		return this.index;
	}

	toString() {
		return '[' + this.index + ']';
	}
}

// private: use Bytes and ByteString.
class ByteArrayBase {
	// private
	constructor(name, o) {
		this.name = name;
		if (o == null) {
			this.bytes = new Uint8Array();
		} else if (o instanceof Uint8Array) {
			this.bytes = o;
		}  else {
	   		throw new Error('expected Uint8Array: ' + o);
		}
	}
	parse(o) {
		if (typeof o === 'string') {
			if (!o.startsWith(this.name + '(') || !o.endsWith(')')) {
		   		throw new Error('expected "' + this.name + '(...) " but was ' + o);
			}
			this.bytes = fromHexWithUnderscores(o.slice(this.name.length + 1, -1));
		}  else {
	   		throw new Error('expected string: ' + o);
		}
		return this;
	}
	getBytes() {
		return this.bytes;
	}	

	toString() {
//		console.log('thisname' + this.name + ' bytes=' + this.bytes);
		return this.name + '(' + toHexWithUnderscores(this.bytes) + ')';
	}
}

// The InfinityDB byte array data type.
// These are used in a list to create blobs. This way,
// blobs can be encoded in JSON when necessary. The com.. are Attributes,
// because they start with '_' and a LC letter. 
// The Bytes() here are 1024 long except the last, which is 
// only as long as needed. A blob is like:
// { 
//	"_com.infinitydb.blob" : {
//		 "_com.infinitydb.blob.mimetype" : "text/plain",
//		 "_com.infinitydb.blob.data" : [
//			"_Bytes(A6_99)"
//		 ]
//    }
// }

class Bytes extends ByteArrayBase {
	constructor(o) {
		super('Bytes', o);
	}
}

// The InfinityDB byte string data type.
// Like Bytes(), but stored in InfinityDB so that it sorts like a string
// whereas Bytes() sorts according to its initial length code.

class ByteString extends ByteArrayBase {
	constructor(o) {
		super('ByteString', o);
	}
}

// Convert 'A6_99' to Uint8Array([0xa6,99]) for Bytes and ByteString

function fromHexWithUnderscores(s) {
	if (s.length == 0) {
		return new Uint8Array();
	}
    if ((s.length + 1) % 3 !== 0)
      throw new Error('Hex has an invalid length - must be 3*n - 1: ' +
      	'length=' + s.length + ' o=' +  s);

    const bytes = new Uint8Array((s.length + 1) / 3);

    for (let i = 0; i + 1 < s.length; i += 3) {
		const hex = s.slice(i, i + 2);
    	const b = parseInt(hex, 16);
	    
	    if (isNaN(b) || hex !== hex.toUpperCase() || b < 0 || b > 0xff) {
	        throw new Error('Hex has an invalid character or is not in uppercase: hex=' + hex + ' b=' + b);
	    }
		if (i + 2 < s.length) {
	        const u = s.charAt(i + 2);
	        if (u !== '_')
	            throw new Error('Expected underscore: ' + u);
	    }
    	bytes[i / 3] = b;
  	}
  	return bytes;
}

// We insist on caps
const hex = '0123456789ABCDEF';

// Convert Uint8Array([0xa6,99]) to 'A6_99' for Bytes and ByteString 

function toHexWithUnderscores(uint8Array) {
  if (typeof uint8Array !== 'object' || !(uint8Array instanceof Uint8Array)) {
    throw new Error("Expected a Uint8Array: " + uint8Array);
  }

  let s = '';
  let isFirst = true;

  for (let i = 0; i < uint8Array.length; i++) {
    const byte = uint8Array[i];

    if (byte < 0 || byte > 0xff) {
      throw new Error("Byte out of range for Uint8Array component: " + byte);
    }

    const hexValue = hex[(byte >> 4) & 0xf] + hex[byte & 0xf];

    if (!isFirst) {
      s += '_';
    }
    isFirst = false;
    
    s += hexValue;
  }
  return s;
}

// The InfinityDB char array data type. Used in CLOBs like
// Bytes, but rarely used.
class Chars {
	constructor(s) {
		if (s == null) {
			this.s = "";
		} else if (typeof s == 'object' && s instanceof Uint8Array) {
			const decoder = new TextDecoder('utf-8');
			this.s = decoder.decode(s);
		} else if (typeof s === 'string') {
			this.s = s;
		} else {
   			throw new Error('expected string or Uint8Array but was ' + o);
   		}
	}
	
	parse(s) {
		if (!s.startsWith('Chars(') || !s.endsWith(')')) {
			throw new Error('"expected "Chars(jsonstring)" but was ' + s);
		}
		this.s = JSON.parse(s.slice(6, -1));
		return this;
	}
	getChars() {
		return this.s;
	}
	
	toString() {
		return 'Chars(' + JSON.stringify(this.s) + ')';
	}
}

class JavaNumber {
	constructor(n) {
		this.n = n;
	}
	value() {
		return this.n;
	}
}

class Double extends JavaNumber {
	constructor(n) {
		super(n);
	}
	// Only for keys: values are left as primitive numbers
	// This format is how InfinityDB distinguishes a double: always a dot
	// Within InfinityDB this is binary
	toString() {
		return Number.isInteger(this.n) ? this.n + '.0' : this.n.toString();
	}
}

class Float extends JavaNumber {
	constructor(n) {
		super(n);
	}
	// This format is how InfinityDB distinguishes a float: always a dot and 'f'
	// Within InfinityDB this is binary
	toString() {
		return Number.isInteger(this.n) ? this.n + '.0' : this.n.toString() + 'f';
	}
}

class Long extends JavaNumber {
	constructor(n) {
		super(n);
		if (!Number.isInteger(n)) {
			throw new Error('Expected integer for Long(n) but was ' + n);
		}
	}
	// This format is how InfinityDB distinguishes a long: never a dot.
	// Within InfinityDB this is binary
	toString() {
		return this.n.toString();
	}
}

// Quote an object of the 12 data types to make it compatible with
// a string object key.

// The default for numbers is always double! You will forget this.
// Longs are most common in InfinityDB rather than integer-valued doubles.
// We can't tell whether to do new Float(o).toString() or qFloat(o)
// or new Long(o).toString() or qLong(o) so you have to do that
// yourself from context..

function qKey(o) {
	if (o === null || o === undefined) {
		throw new TypeError('keys must not be null');
	} else if (typeof o === 'boolean') {
		return o ? '_true' : '_false';
	} else if (typeof o === 'number') {
		return '_' + new Double(o).toString();
	} else if (typeof o === 'string') {
		return o.charAt(0) !== '_' ? o : '_' + o;
	} else if (o instanceof Date) {
		return '_' + o.toISOString();
	} else if (o instanceof EntityClass 
		|| o instanceof Attribute
		|| o instanceof Bytes
		|| o instanceof ByteString 
		|| o instanceof Chars
		|| o instanceof Index
		|| o instanceof Double
		|| o instanceof Float
		|| o instanceof Long) {
		return '_' + o.toString();
	} else if (o instanceof Uint8Array) {
		return '_' + new Bytes(o).toString();
	} else {
		throw new TypeError('Uknown data type: cannot underscore-quote: ' + o);
	}
}


// Quote something of the 12 data types to make it compatible with
// an object value. We don't change boolean or number into
// strings though, which means InfinityDB will interpret the
// number as a double! If you want to have the number underscore
// quoted, use qLong(n) or qFloat(n) to generate '_5' or '_5.0f'.
// Note that a Java Float is not the same as a JavaScript float.

function qValue(o) {
	if (o === null || o === undefined) {
		return null;
	} else if (typeof o === 'boolean' || typeof o === 'number') {
		// default is always double! You will forget this.
		// We can't tell whether to do new Float(o).toString()
		// or new Long(o).toString() so you have to do that
		// yourself from context..
		return o;
	} else {
		return qKey(o);
	}
}

// Unquote an underscore-quoted value
// This works for keys or values
// Be careful with numbers: InfinityDB long and float become Long and Float
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
	} else if (isDigit(o.charAt(0))) {
		if (isValidIsoDate(o)) {
			return new Date(o);
		}
		if (o.includes('.')) {
			if (o.endsWith('f')) {
				if (isNaN(o.slice(0, -1))) {
					throw new Error("expected float but was " + o);
				}
				return new Float(Number.parseFloat(o.slice(0,-1)));
			}
			if (isNaN(o)) {
				throw new Error("expected double but was " + o);
			}
			// Note we don't return a new Double() here!
			return Number.parseFloat(o);
//			return new Double(Number.parseFloat(o));
		} else {
			if (isNaN(o)) {
				throw new Error("expected long but was " + o);
			}
			return new Long(Number.parseInt(o));
		}
	} else if (!o.includes('(')) {
		if (EntityClass.isValidEntityClass(o)) {
			return new EntityClass(o);
		} else if (Attribute.isValidAttribute(o)) {
			return new Attribute(o);
		} else if (o.startsWith('[')) {
			return new Index().parse(o);
		} else {
			throw new Error("expected entity class or attribute but was " + o);
		}
	} else if (o.startsWith('Bytes(')) {
		return new Bytes().parse(o);
	} else if (o.startsWith('ByteString(')) {
		return new ByteString().parse(o);
	} else if (o.startsWith('Chars(')) {
		return new Chars().parse(o);
	} else if (o.startsWith('Index(')) {
		return new Index().parse(o);
	} else {
		throw new TypeError('cannot underscore-unquote value=' + o);
	}
}

function isDigit(o) {
	return typeof o === 'string' && '0123456789'.includes(o.charAt(0));
}

function isValidIsoDate(dateString) {
	const isoDateRegex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?)(Z|[-+]\d{2}:\d{2})?$/;
	return isoDateRegex.test(dateString);
}

function qDouble(n) {
	return '_' + new Double(n).toString();
}

function qLong(n) {
	return '_' + new Long(n).toString();
}

function qFloat(n) {
	return '_' + new Float(n).toString();
}

// Demonstrate some fundametal ways to work with the 12 data types.
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
		// Use qFloat() to add the '_' and '.' and 'f' to force float on the InfinityDB side
		[qFloat(5.0)]: '_5.0f',
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
		[qKey(false)]: false,
		[qKey(new Uint8Array())]: '_Bytes()',
		[qKey(new Bytes(new Uint8Array([1,0xa6])))]: '_Bytes(01_A6)',
		[qKey(new ByteString(new Uint8Array([1,0xa6])))]: '_ByteString(01_A6)',
		// Slight problem with backslashing backslashes..
		[qKey(new Chars('x\'\\\"'))]: '_Chars("x\'\\\"")',
		[qKey(new Index(5))]: '_[5]',
		[qKey(new Index(5))]: '_[5]',
	};
	return object;
}

function printType(o) {
	if (typeof o === 'string') {
		return 'string';
	} else if (typeof o === 'boolean') {
		return 'boolean';
	} else if (typeof o === 'number') {
		return 'number';
	} else if (typeof o === 'object') {
   		return o.constructor.name;
	} else if (o === null) {
		return 'null';
	} else if (o === undefined) {
		return '(undefined)';
	} else {
		return o;
	}
}

function exampleUnquoteObject(o) {
	for (const key in o) {
		const k = uq(key);
		const v = uq(o[key]);
		const tk = printType(k);
		const tv = printType(v);
		console.log(`Key: ${tk}:${k}, Value: ${tv}:${v}`);
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
