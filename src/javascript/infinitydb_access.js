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

// Here are the classes that represent the 12 InfinityDB data types.
// The most general InfinityDB class of data elements is the Component.
// By definition, a component is any of the 12 data types,
// However, instead of subclasses of Component, the JavaScript
// primitive types number, string, boolean and Date are handled 
// separately. JS number is always handled as a double (which you 
// will forget.) So, we provide optional IdbFLoat, IdbLong, and 
// IdbDouble to hold a number that will retain its InfinityDB meaning.


// All of this code is really optional and is provided in the
// hope it will be useful. You can do everything by hand.
// Parsing IdbBlobs is pretty useful.

class IdbComponent {
	
	// v always has the value of any subclass.
	
	constructor(v) {
		this.v = v;
	}
	
	toJSON() {
		return toString();		
	}

	// We consider our special InfinityDB classes to be elementary - like JS primitives
	// For InfinityDB, a 'Component' is either a Primitive or Meta and
	// all elementary values are Components. An 'Item' is a short series of these.
	
	static isComponent(o) {
		return o !== null && o !== undefined &&
			(typeof o === 'number'
			|| typeof o === 'boolean' 
			|| typeof o == 'string'
			|| o instanceof Date 
			|| o instanceof IdbComponent);
	}
}

// All but IdbMetas, string, number, boolean and Date.
// Numbers can either be primitives or subclasses of IdbNumber.
// Inside the DB, this includes all but Metas. 

class IdbPrimitive extends IdbComponent {
	constructor(v) {
		super(v);
	}
}

// The Metas are the non-primitive data types and delimit the 'data'
// parts.

class IdbMeta extends IdbComponent {
	constructor(name) {
		super(name);
	}
	
	toString() {
		return this.v;
	}
}

// Instead of these, you can also write { _MyClass : 5; }

class IdbClass extends IdbMeta {
	constructor(name) {
		super(name);
		if (!IdbClass.isValidName(name)) {
			throw new TypeError('IdbClasses must be UC, then letters, digits, dots, and underscores');
		}
	}

	static isValidName(s) {
		const metaRegex = /^[A-Z][a-zA-Z\d\._]*$/;
		return metaRegex.test(s);
	}
}

// Instead of these, you can also write { _myAttribute : 5; }

class IdbAttribute extends IdbMeta {
	constructor(name) {
		super(name);
		if (!IdbAttribute.isValidName(name)) {
			throw new TypeError('IdbAttributes must be LC, then letters, digits, dots, and underscores');
		}
	}
	
	static isValidName(s) {
		const metaRegex = /^[a-z][a-zA-Z\d\._]*$/;
		return metaRegex.test(s);
	}
}

// Dates are handled separately even though they are InfinityDB
// 'primitive' types.

function idbIsValidIsoDate(dateString) {
	const isoDateRegex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?)(Z|[-+]\d{2}:\d{2})?$/;
	return isoDateRegex.test(dateString);
}

// Instead of this you can also write '_[n]' like '_[55]' or '_Index(55)'. 
// An Index is one of 12 InfinityDB types, and when it occurs in an Item,
// it indicates that the JSON form of it is an array. For example, in
// the token form of an Item, it might be:
// "abc" [55] myAttribute. 
// In a URL quoted by idbUriQuote it might be: 
// ['abc',new IdbIndex(55),new IdbAttribute('myAttribute')],
// and in accessing a JS object, it would be x.abc[55]._myAttribute.

class IdbIndex extends IdbPrimitive {
	constructor(index) {
		super(index);
		if (index == null) {
			index = 0;
		} else if (typeof index !== 'number') {
   			throw new TypeError('Constructing an Index: expected a number but was ' + index);
		}
	}
	parse(s) {
		if (typeof s === 'string') {
			if (s.startsWith('Index(') && s.endsWith(')')) {
				s = s.slice(6, -1);
			} else if (s.startsWith('[') && s.endsWith(']')) {
				s = s.slice(1, -1);
			} else {
			   	throw new TypeError('expected string to parse as Index(n) or [n] but was ' + s);
			}
		} else {
			 throw new TypeError('expected string to parse an Index but was ' + s);
		}
		this.v = Number.parseInt(s);
		if (isNaN(this.v) || !Number.isInteger(this.v)) {
			throw new TypeError('expected "Index(n) but n is not an integer: it was "' + this.v);
		}
		return this;
	}
	
	toString() {
		return '[' + this.v + ']';
	}
}
// We insist on caps
const idbHexChars = '0123456789ABCDEF';

// No corresponding InfinityDB type: use Bytes and ByteString.

class IdbByteArray extends IdbPrimitive {

	// protected access
	constructor(bytes, name) {
		super(bytes);
		this.name = name;
		if (bytes == null) {
			this.v = new Uint8Array(0);
		} else if (bytes instanceof Uint8Array) {
			this.v = bytes;
		}  else {
	   		throw new TypeError('expected Uint8Array: ' + bytes);
		}
	}
	parse(o) {
		if (typeof o === 'string') {
			if (!o.startsWith(this.name + '(') || !o.endsWith(')')) {
		   		throw new TypeError('expected "' + this.name + '(...) " but was ' + o);
			}
			this.v = IdbByteArray.fromHexWithUnderscores(o.slice(this.name.length + 1, -1));
		}  else {
	   		throw new TypeError('expected string: ' + o);
		}
		return this;
	}

	toString() {
		return this.name + '(' + IdbByteArray.toHexWithUnderscores(this.v) + ')';
	}
	
	toJSON() {
		return '_' + this.name + '(' + IdbByteArray.toHexWithUnderscores(this.v) + ')';
	}
	
	// Convert 'A6_99' to Uint8Array([0xa6,99]) for Bytes and ByteString

	static fromHexWithUnderscores(s) {
		if (s.length == 0) {
			return new Uint8Array();
		}
	    if ((s.length + 1) % 3 !== 0)
	      throw new TypeError('Hex has an invalid length - must be 3*n - 1: ' +
	      	'length=' + s.length + ' o=' +  s);
	
	    const bytes = new Uint8Array((s.length + 1) / 3);
	
	    for (let i = 0; i + 1 < s.length; i += 3) {
			const hex = s.slice(i, i + 2);
	    	const b = Number.parseInt(hex, 16);
		    
		    if (isNaN(b) || hex !== hex.toUpperCase() || b < 0 || b > 0xff) {
		        throw new TypeError('Hex has an invalid character or is not in uppercase: hex=' + hex + ' b=' + b);
		    }
			if (i + 2 < s.length) {
		        const u = s.charAt(i + 2);
		        if (u !== '_')
		            throw new TypeError('Expected underscore: ' + u);
		    }
	    	bytes[i / 3] = b;
	  	}
	  	return bytes;
	}
	
	// Convert Uint8Array([0xa6,99]) to 'A6_99' for Bytes and ByteString 
	
	static toHexWithUnderscores(uint8Array) {
	  if (typeof uint8Array !== 'object' || !(uint8Array instanceof Uint8Array)) {
	    throw new TypeError("Expected a Uint8Array: " + uint8Array);
	  }
	
	  let s = '';
	  let isFirst = true;
	
	  for (let i = 0; i < uint8Array.length; i++) {
	    const byte = uint8Array[i];
	
	    if (byte < 0 || byte > 0xff) {
	      throw new TypeError("Byte out of range for Uint8Array component: " + byte);
	    }
	
	    const hexValue = idbHexChars[(byte >> 4) & 0xf] + idbHexChars[byte & 0xf];
	
	    if (!isFirst) {
	      s += '_';
	    }
	    isFirst = false;
	    
	    s += hexValue;
	  }
	  return s;
	}
}

// The InfinityDB byte array data type. These are short, and must fit in
// an Item when encoded, which is 1665 chars long.

class IdbBytes extends IdbByteArray {
	constructor(o) {
		super(o, 'Bytes');
	}
}

// The InfinityDB byte string data type.
// Like Bytes(), but stored in InfinityDB so that it sorts like a string
// whereas Bytes() sorts according to its initial length code.
// These are short, and must fit in an Item when encoded, which is 1665 chars long.

class IdbByteString extends IdbByteArray {
	constructor(o) {
		super(o, 'ByteString');
	}
}

// The InfinityDB char array data type. Can be in CLOBs like
// Bytes, but rarely used.
// These are short, and must fit in an Item when encoded, which is 1665 chars long.

class IdbChars extends IdbPrimitive {
	constructor(s) {
		super(s);
		if (s == null) {
			this.v = "";
		} else if (typeof s == 'object' && s instanceof Uint8Array) {
			const decoder = new TextDecoder('utf-8');
			this.v = decoder.decode(s);
		} else if (typeof s === 'string') {
			this.v = s;
		} else {
   			throw new TypeError('expected string or Uint8Array but was ' + o);
   		}
	}
	
	parse(s) {
		if (!s.startsWith('Chars(') || !s.endsWith(')')) {
			throw new TypeError('"expected "Chars(jsonstring)" but was ' + s);
		}
		this.v = JSON.parse(s.slice(6, -1));
		return this;
	}
	
	toString() {
		return 'Chars(' + JSON.stringify(this.v) + ')';
	}
}

// There is corresponding type within the db: use IdbLong, IdbDouble, or IdbFloat
// subclasses.

class IdbNumber extends IdbPrimitive {
	constructor(n) {
		super(n);
	}

	static isDigit(o) {
		return typeof o === 'string' && '0123456789'.includes(o.charAt(0));
	}
}

class IdbDouble extends IdbNumber {
	constructor(n) {
		super(n);
	}

	// This format is how InfinityDB identifies a double: always a decimal
	// point. Normal JavaScript numbers are interpreted as doubles.
	// Within InfinityDB this is binary and distinguished from the others.

	toString() {
		return Number.isInteger(this.v) ? this.v + '.0' : this.v.toString();
	}
}

class IdbFloat extends IdbNumber {
	constructor(n) {
		super(n);
	}

	// This format is how InfinityDB identifies a float: always a 
	// decimal point and 'f'. Within InfinityDB this is binary and
	// distinguished from long and double.
	
	toString() {
		return (Number.isInteger(this.v) ? this.v + '.0' : this.v.toString())  + 'f';
	}
}

class IdbLong extends IdbNumber {
	constructor(n) {
		super(n);
		if (!Number.isInteger(n)) {
			throw new TypeError('Expected integer for Long(n) but was ' + n);
		}
	}
	
	// This format is how InfinityDB distinguishes a long: no decimal point.
	// Within InfinityDB this is binary.
	
	toString() {
		return this.v.toString();
	}
}

// Quote an object of the 12 data types to make it compatible with
// a string object key. All of the component types have an underscore
// placed in front except strings. A string that happens to already
// have an underscore is quoted by 'stuffing in' another one.

function idbKey(o) {
	if (o === null || o === undefined) {
		throw new TypeError('keys must not be null');
	} 
	if (typeof o === 'string') {
		return o.charAt(0) !== '_' ? o : '_' + o;
	} 
	return '_' + idbToToken(o);
}

// Quote something of the 12 data types to make it compatible with
// an object value. We don't change number into
// strings though, which means InfinityDB will interpret the
// number as a double! If you want to have the number underscore
// quoted, use IdbDoubleKey(), IdbLongKey(n) or IdbFloatKey(n) 
// to generate '_5.0' '_5' or '_5.0f' to correspond to the types
// within InfinityDB. We also provide IdbFloat, IdbDouble, and
// IdbLong classes that remember their
// types, and you can get the contained number with myNumber.v.

function idbValue(o) {
	if (o === undefined) {
		throw new TypeError('idbValue(undefined)');
	} else if (o === null || typeof o === 'boolean' || typeof o === 'number') {
		return o;
	} else {
		return idbKey(o);
	}
}

// Unquote an underscore-quoted key or value.
// Be careful with numbers: InfinityDB Long, Float and Double
// become the corresponding classes IdbDouble, IdbLong or 
// IdbFloat here , so you may have to use mylong.v. 
// Non-string JS primitives are just returned.

function idbUnQuote(o) {
	if (o === undefined) {
		throw new TypeError('idbUnQuote(undefined)');
	} else if (typeof o !== 'string') {
		return o;
	} else if (o === '') {
		return '';
	} else if (o.charAt(0) !== '_') {
		return o;
	} else if (o.startsWith('__')) {
		return o.slice(1);
	}
	// it is a string starting with a single underscore
	return idbFromToken(o.slice(1));
}

// The InfinityDB standard way to represent each data type as a string.

function idbToToken(o) {
	if (o === null || o === undefined) {
		throw new TypeError('There is no null data type in InfinityDB');
	} else if (typeof o === 'boolean') {
		return o ? 'true' : 'false';
	} else if (typeof o === 'number') {
		return new IdbDouble(o).toString();
	} else if (typeof o === 'string') {
		return JSON.stringify(o);
	} else if (o instanceof Date) {
		return o.toISOString();
	} else if (o instanceof IdbComponent) {
		return o.toString();
	} else if (o instanceof Uint8Array) {
		return new IdbBytes(o).toString();
	} else {
		throw new TypeError('Uknown data type: cannot create token form of : ' + o);
	}
}

// Parse a standard InfinityDB token that represents a single
// component of the 12 data types, like Class, Attribute, 
// String, Long, Boolean, Date etc. When Items are printed by
// default they just come out as a sequence of these tokens.

// Note that the three numeric types come out as instances of IdbLong, IdbDouble, and
// IdbFloat. This way they preserve their types for later writing back.
// You can get the value with myfloat.v. Sometimes you will find regular
// JavaScript numbers, and these are interpreted as doubles, although
// longs are more common in the database, so be careful.

function idbFromToken(o) {	
	if (o === 'null') {
		return null;
	} else if (o === 'true') {
		return true;
	} else if (o === 'false') {
		return false;
	} else if (IdbNumber.isDigit(o.charAt(0))) {
		if (idbIsValidIsoDate(o)) {
			return new Date(o);
		}
		if (o.includes('.')) {
			if (o.endsWith('f')) {
				if (isNaN(o.slice(0, -1))) {
					throw new TypeError("expected float but was " + o);
				}
				return new IdbFloat(Number.parseFloat(o.slice(0,-1)));
			}
			if (isNaN(o)) {
				throw new TypeError("expected double but was " + o);
			}
			return new IdbDouble(Number.parseFloat(o));
		} else {
			if (isNaN(o)) {
				throw new TypeError("expected long but was " + o);
			}
			return new IdbLong(Number.parseInt(o));
		}
	} else if (!o.includes('(')) {
		if (o.startsWith('[')) {
			return new IdbIndex().parse(o);
		} else if (idbUpperCaseRegex.test(o.charAt(0))) {
			return new IdbClass(o);
		} else {
			return new IdbAttribute(o);
		}
	} else if (o.startsWith('Bytes(')) {
		return new IdbBytes().parse(o);
	} else if (o.startsWith('ByteString(')) {
		return new IdbByteString().parse(o);
	} else if (o.startsWith('Chars(')) {
		return new IdbChars().parse(o);
	} else if (o.startsWith('Index(')) {
		return new IdbIndex().parse(o);
	} else {
		throw new TypeError('cannot underscore-unquote value=' + o);
	}
}
const idbUpperCaseRegex = /^[A-Z]$/;

function idbDoubleKey(n) {
	return '_' + new IdbDouble(n);
}

function idbFloatKey(n) {
	return '_' + new IdbFloat(n);
}

function idbLongKey(n) {
	return '_' + new IdbLong(n);
}

// A blob is data plus a string contentType, which
// is any internet 'mime type' including but not limited to
// 'text/plain', 'application/json' or 'image/jpeg'.
// The data can be a Uint8Array or a string or a pre-parsed JSON object.
// The I/O uses IdbBlobs for speed.

// We can also parse a JSON object into a Uint8Array and contentType starting with a
// JSON object of a particular structure for InfinityDB. This means we
// can encode and transfer multiple arbitrarily long binary data chunks
// inside JSON, along with other structure in one I/O. The data part is an aarray 
// of IdbBytes() which are 1024 long except the last, which is 
// only as long as needed. 

// Here is the structure:
// { 
//	"_com.infinitydb.blob" : {
//		 "_com.infinitydb.blob.mimetype" : "text/plain",
//		 "_com.infinitydb.blob.data" : [
//			"_Bytes(A6_99)"
//		 ]
//    }
// }

// Not a subclass of IdbComponent.

class IdbBlob {
	
	constructor(data, contentType) {
		this.v = data;
		this.contentType = contentType;
		if (data != null) {
			if (typeof contentType !== 'string') {
				throw new TypeError('new IdbBlob() contentType must be a string' +
					' but was ' + idbPrintType(contentType));
			}
//			console.log("blob data type " + idbPrintType(data));
			if (typeof data === 'string') {
				if (contentType === 'application/json') {
					this.v = JSON.parse(data);
				} else if (!contentType.startsWith('text/')) {
					throw new TypeError('new IdbBlob() string data requires a' +
						' content type of "text/..." but was ' + contentType);
				}
			} else if (data instanceof Uint8Array) {
				// v already set
			} else if (data instanceof ArrayBuffer) {
				this.v = new Uint8Array(data);
			} else if (typeof data === 'object') {
				// The JSON is already -parsed for us.
				if (contentType !== 'application/json') {
					throw new TypeError('new IdbBlob() object data requires a' + 
						' content type of "application/json" but was ' + contentType);
				}
			} else {
				throw new TypeError('new IdbBlob() object data of incompatible type: ' + idbPrintType(data));
			}
		}
	}

	// Get the content and return it converted to a JSON object.
	// Returns the content unchanged if possible with shallow copy.
	// Preserves this.

	getAsParsed() {
		if (this.contentType !== 'application/json') {
			throw new TypeError('Cannot get the JSON from a blob: content type was ' + this.contentType);
		} 
		if (this.v instanceof Uint8Array) {
			const decoder = new TextDecoder('utf-8');
			return  JSON.parse(decoder.decode(this.v));
		} else if (typeof this.v === 'string') {
			return JSON.parse(this.v);
		} else {
			return this.v;
		}
	}

	// Set the value this.v as a Uint8Array by parsing it from a JSON object 
	// structured in the InfinityDB way.
	// This sets the contentType as well. This allows us to
	// encode binary data into JSON, and that means we can transfer
	// multiple blobs per I/O along with any other structure.
	// The other way to transfer a blob is one at a time
	// directly as a Uint8Array in one fast binary operation from
	// any given Uri i.e. Item prefix that identifies a blob in the db. 
	
	parseFromBlobStructure(o) {
		if (o == null || typeof o !== 'object') {
			throw new TypeError('Expected a blob structured object but was ' + idbPrintType(o));
		}
		const blobInternals = o['_com.infinitydb.blob'];
		if (blobInternals == null) {
			return null;
		}
		if (typeof blobInternals !== 'object') {
			throw new TypeError('Expected a blob structured object but was' + 
				' missing attribute com.infinitydb.blob and was ' + idbPrintType(blobInternals));
		}
		const contentType = blobInternals['_com.infinitydb.blob.mimeType'];
		if (typeof contentType !== 'string') {
			throw new TypeError('Expected a blob structured object but was missing' +
				' attribute com.infinitydb.blob.mimeType: ' + idbPrintType(contentType));
		}
		const array = blobInternals['_com.infinitydb.blob.bytes'];
		if (array == null || !Array.isArray(array)) {
			throw new TypeError('Expected a blob structured object but was' +
				' missing attribute com.infinitydb.blob.bytes ' + idbPrintType(array));
		}
		
		var theFullData = null;
		if (array.length === 0) {
			theFullData = new Uint8Array(0);
		} else {
			const chunks = [];
			for (const b of array)  {
				const idbBytes = new IdbBytes().parse(b.slice(1));
				chunks.push(idbBytes.v);
			} 
			theFullData = Buffer.concat(chunks);
			if (!(theFullData instanceof Uint8Array)) {
				theFullData = new Uint8Array(theFullData);
			}
		}
		this.v = theFullData;
		this.contentType = contentType;
		return this;
	}
	
	// Preserves this.
	toBlobStructure() {
		if (!(this.v instanceof Uint8Array)) {
			throw new TypeError('Cannot convert to blob structure: data is not a Uint8Array ' + idbPrintType(this.v));
		}
		if (typeof this.contentType !== 'string') {
			throw new TypeError('Cannot convert to blob structure: missing contentType string: was ' + idbPrintType(this.contentType));
		}
		const bytesArray = [];
		for (let i = 0; i < this.v.length; i += 1024) {
			const chunk = this.v.slice(i, i+ 1024);
			bytesArray.push(new IdbBytes(chunk).toJSON());
		}
		const structure = { 
			'_com.infinitydb.blob': {
			 	'_com.infinitydb.blob.bytes': bytesArray,
			 	'_com.infinitydb.blob.mimeType': this.contentType
			 }
		}
		return structure;
	}

	static isBlob(o) {
		if (o == null || typeof o !== 'object') {
			return false;
		}
		if (o instanceof IdbBlob) {
			return true;
		}
		const blobInternals = o['_com.infinitydb.blob'];
		return blobInternals != null;
	}
	
	toJSON() {
		console.log(JSON.stringify(this.toBlobStructure()));
		return JSON.stringify(this.toBlobStructure());
	}
	
	// If  we are in a text kind of contentType like text/plain,
	// return this.v, or  decode it as a Uint8Array into text using 
	// UTF-8, or if it is an object, do JSON stringify.
	
	toString() {
		if (typeof this.v === 'string') {
			return this.v;
		} else if (this.v instanceof Uint8Array) {
			const decoder = new TextDecoder('utf-8');
			const s = decoder.decode(this.v);
			return  s;
		} else if (typeof this.v === 'object') {
			if (this.contentType !== 'application/json') {
				throw new TypeError('Found a blob containing an object' +
					' but is is not application/json: ' + this.contentType);
			}
			return JSON.stringify(this.v);
		} else {
			throw new TypeError('IdbBlob contains incompatible data of type ' +
				idbPrintType(this.v) + ' content type is ' + this.contentType);
		}
	}
	
	// From an object or array, recursively construct a new one but
	// with all possible IdbBlobs instead of their object structures.
	// Then you can get their Uint8Array forms directly, such as for
	// images.
	
	static idbConvertAllContainedBlobStructures(o) {
		if (o === undefined) {
			throw new TypeError('Undefined object');
		}
		if (o === null || typeof o !== 'object' || o instanceof IdbComponent) {
			return o;
		}
		if (Array.isArray(o)) {
			const converted = [];
			for (const e of o) {
				converted.push(IdbBlob.idbConvertAllContainedBlobStructures(e));
			}
			return converted;
		}
		const isIdbBlob = o['_com.infinitydb.blob'] != null; 
		if (isIdbBlob) {
			const idbBlob = new IdbBlob().parseFromBlobStructure(o);
//			console.log('IdbBlob ' + idbBlob);
			console.log('IdbBlob len ' + idbBlob.v.length);
			return idbBlob;
		}
		const converted = {};
		for (const k in o) {
			if (!o.hasOwnProperty(k))
				continue;
			const v = o[k];
			converted[k] = IdbBlob.idbConvertAllContainedBlobStructures(v);
		}
		return converted;
	}
}

// Use this to create the URI path following the host and database:
// in other words the part that gets turned into an Item at the server.
// Components is a varargs of a possibly nested array of encoded components
// Each compoent should be:
//   * a string, which gets string quoted like "mystring"
//   * a number, which is interpreted as a double
//   * a boolean which turns into true or false, or
//   * a subclass of IdbComponent, which will be encoded properly.
// You can do this by hand without much trouble. 

function idbEncodeUri(...components) {
    s = '';
    // Flatten
    const componentsFlat = [].concat(components);
    for (const component of componentsFlat) {
       	s += '/' +  idbEncodeUriComponent(component);
    }
    return s;
}

// Using a JS number defaults to double, which may not be obvious.
// You can use the IdbLong, IdbDouble, and IdbFloat classes for clarity.
// The main point is to prevent uri-quoting the slashes. You can 
// do this by hand in many cases.

function idbEncodeUriComponent(s) {
	if (s === null || s === undefined) {
		throw new TypeError("Cannot encode a uri of null");
	} else if (typeof s === 'string') {
        s = JSON.stringify(s);
    } else if (typeof s == 'number' || typeof s == 'boolean' || s instanceof IdbComponent) {
		s = s.toString();
	}
	// The built-in function
    return encodeURIComponent(s);
}

// Get a string representing the type of any value
function idbPrintType(o) {
	if (o === null) {
		return 'null';
	} else if (o === undefined) {
		return '(undefined)';
	} else if (typeof o === 'string' || typeof o == 'boolean' || typeof o == 'number') {
		return typeof o;
	} else if (Array.isArray(o)) {
		return 'array';
	} else if (typeof o === 'object') {
   		return o.constructor.name;
	} else {
		return o;
	}
}

// DEMONSTRATION CODE

// Demonstrate some fundametal ways to work with the 12 data types.
// You can do this by hand easily as well.

function demonstrateCreateObject() {
	const o = 'anything';
	// any string at all
	const s = '_string already  starting with underscore';
	// classes start with UC, then any letters, digits, underscores or dots
	// These are really optional - you can just add an underscore except for dots
	const C = new IdbClass('C_a.b');
	// classes start with LC, then any letters, digits, underscores or dots
	const a = new IdbAttribute('a_a.b');
	const n = 5;

	const object = {
		
		// Quote any type! This is actually all you need except for dealing with
		// the number ambiguity in which there are three InfinityDB number types.
		
		[idbKey(o)]: idbValue(o),
		
		// This forces purely ISO dates
		
		[idbKey(new Date())]: idbValue(new Date()),
		// The primitive always prints iso
		['_2023-09-17T21:24:35.653Z']: idbValue(new Date('2023-09-17T21:24:35.653Z')),
		['_2023-09-17T21:24:35.653-07:00']: idbValue(new Date('2023-09-17T21:24:35.653-07:00')),

		// Use idbLongKey() to add the '_' to force long, which is a 64-bit integer
		// on the InfinityDB side
		
		_5: '_5',
		[idbLongKey(n)]: idbLongKey(n),
		// Use idbFloatKey() to add the '_' and '.' and 'f' to force float on the InfinityDB side
		[idbFloatKey(5.0)]: '_5.0f',
		[idbFloatKey(n)]: idbFloatKey(n),
		// Use the default double for numbers.
		// You will forget that double is default, not long!!
		// Even if the value is integer, because there are three number types.
		'_5.0': 5,
		'_5.1': 5.1,
		'_5.1': '_5.1',
		[idbKey(n)]: n,
		// Here x is a string on the InfinityDB side, not an attribute or class
		'x': 'x',
		// here _C is a class without the '_' on the InfinityDB side
		'_C_.a': '_C_.a',
		// Here _a is an attribute without the '_' on the InfinityDB side
		'_a._a': '_a._a',
		// here __s is a string starting with single quote '_' on the InfinityDB side
		// s is unknown string automatically underscore quoted if necessary
		[idbKey(s)]: idbValue(s),
		// Unknown class or attribute
		[idbKey(C)]: idbValue(C),
		[idbKey(a)]: idbValue(a),
		[idbKey(true)]: true,
		[idbKey(false)]: false,
		[idbKey(new Uint8Array())]: '_Bytes()',
		[idbKey(new IdbBytes(new Uint8Array([1,0xa6])))]: '_Bytes(01_A6)',
		[idbKey(new IdbByteString(new Uint8Array([1,0xa6])))]: '_ByteString(01_A6)',
		// Slight problem with backslashing backslashes..
		[idbKey(new IdbChars('x\'\\\"'))]: '_Chars("x\'\\\"")',
		[idbKey(new IdbIndex(5))]: '_[5]',
		[idbKey('subtree')] : {
			[idbKey(new IdbDouble(3))]: 'double',
			[idbKey(6)]: 'double',
			[idbKey(6.0)]: 'double',
			[idbKey(6.1)]: 'double',
			[idbKey(new IdbLong(7))]: 'long',
			[idbKey(new IdbFloat(3))]: 'float',
			[idbKey('null value')] : null
		}
	};
	return object;
}

function demonstrateUnQuoteObject(o) {
	for (const key in o) {
		const k = idbUnQuote(key);
		const v = idbUnQuote(o[key]);
		const tk = idbPrintType(k);
		const tv = idbPrintType(v);
		console.log(`Key: ${tk}:${k}, Value: ${tv}:${v}`);
	}
}

// This demonstrates recursing over a JSON tree in order to print
// a JSON tree. Not very useful, but it can print also in the
// InfinityDB extended JSON form, where we minimize quotes.
// It shows using the unquoter idbUnQuote() to get the key out,
// and the quoter idbKey() to re-create it ready for use as a key.
// Also, if we have already run idbConvertAllContainedBlobs, then
// the tree contains Uint8Arrays, and we want to avoid them
// printing literally - they convert back to the original form.

// indent is a string to concatenate depth times

function idbPrintAsJSON(o, depth, isExtended, indentString) {
	if (o == null) {
		return 'null';
	}
	if (depth == null) {
		depth = 0;
	}
	if (isExtended == null) {
		isExtended = false;
	}
	var s = '';
	var isFirst = true;
	if (IdbComponent.isComponent(o)) {
		return isExtended ? idbToToken(idbUnQuote(o)) : JSON.stringify(o);
	} else if (o instanceof IdbBlob) {
		return idbPrintAsJSON(o.toBlobStructure(), depth, isExtended, indentString);
	} else if (Array.isArray(o)) {
		s += '[\r\n';
		for (var e of o) {
			if (!isFirst) {
				s += ',\r\n';
			}
			isFirst = false;
			s += indent(depth + 1, indentString);
			if (e !== null) {
				s += idbPrintAsJSON(e, depth  + 1, isExtended, indentString);
			} else {
				s += 'null';
			}
		}
		s += '\r\n' + indent(depth) + ']';
	} else if (typeof o == 'object') {
		s += '{\r\n';
		for (var k in o) {
			if (!isFirst) {
				s += ',\r\n';
			}
			isFirst = false;
			s += indent(depth + 1, indentString);
			// extract from the form in the input JSON
			s += (isExtended ? idbToToken(idbUnQuote(k)) : JSON.stringify(k)) + ': ';
			const v = o[k];
			s += idbPrintAsJSON(v, depth  + 1, isExtended, indentString);
		}
		s +=  '\r\n' + indent(depth, indentString) + '}';
	} else {
		
	}
	return s;
}

function indent(n, indention) {
	if (indention == null) {
		indention = '    ';
	}
	var s = '';
	for (var i = 0; i < n; i++) {
		s += indention;
	}
	return s;
}

// THE REST INTERFACES

// There are quite a few such interfaces available to nodejs.
// See test.html also for how it is done in a browser with AJAX i.e. XHTML.

function main() {

	// All of the code is really optional and is provided in the
	// hope it will be useful. You can do everything by hand, but
	// parsing IdbBlobs is pretty useful.

	// Demo the underscore-quoting functions

	const object = demonstrateCreateObject();
	console.log(JSON.stringify(object));
	demonstrateUnQuoteObject(object);

	// Demo the recursive descent access of an object
	// with underscore-quoted keys in order to handle the
	// 12 InfinityDB datatypes as key strings.
		
	console.log(idbPrintAsJSON(object, 0, true));
	
	// Check the data received through the REST connection.
	
	function testReceivedData(blob) {
		checkConsistency(blob);
		// How to look inside the object if it is the documentation.
//		console.log('From https: ' + blob.getAsParsed().Basics._description[0]);
		if (blob.contentType == 'application/json') {
			// recursively replace all IdbBlobs from their structures in the
			// already parsed JSON.
			o = blob.getAsParsed();
			const converted = IdbBlob.idbConvertAllContainedBlobStructures(o);
			console.log('converted');
			convertedPrinted = idbPrintAsJSON(converted)
//			console.log(convertedPrinted);
//			console.log(JSON.stringify(converted));
			checkConsistency(convertedPrinted);
		}
	}
	
	// Do a couple of rounds back and forth between object and JSON forms.
	function checkConsistency(o) {
		var objectsParsed = o;
		if (o.contentType == 'application/json') {
			// o is a JSON blob. Get the object tree
			objectsParsed = o.getAsParsed();
		}
		// Use the recursive printer we defined.
		const jsonPrinted = idbPrintAsJSON(objectsParsed);
//		console.log('in check consistency: ' + jsonPrinted);
		// Go through another cycle
		const objectsReparsed = JSON.parse(jsonPrinted);
		const jsonPrinted2 = idbPrintAsJSON(objectsReparsed);
		console.log(jsonPrinted !== jsonPrinted2 ? "inconsistent" : "consistent");
		return jsonPrinted;
	}

	// Some test URIs. Constant ones can alternatively just be url-quoted by hand
	
	// Read all of the queries as application/json. This is good exercize as all types are used.
	const queryUri = idbEncodeUri(new IdbClass('Query'));
	// The entire documentation as application/json
	const documentationsUri = idbEncodeUri(new IdbClass('Documentation'));
	// One picture, as binary image/jpeg - 1.7MB, fast, compressed.
	const picturesPic0Uri = idbEncodeUri(new IdbClass('Pictures'),'pic0');
	// All of the pictures as application/json - 35MB, not very compressed
	// These need to be parsed as multiple binary blobs out of their JSON form.
	const allPicturesUri = idbEncodeUri(new IdbClass('Pictures'));
	// Where we write things
	const trashJavaScriptDemoUri = idbEncodeUri(new IdbClass('Trash'),new IdbClass('JavaScriptDemo'));

	const protocol = 'https';
//	const protocol = 'http';
	const host = 'infinitydb.com';
//	const host = 'localhost';
	const port = 37411;
//	const db = '/demo/readonly';
	const db = '/demo/writeable';
	const uri = queryUri;
	
	const axios = require('axios');
	let https;
	let http;
	try {
		https = require('node:https');
		http = require('node:http');
	} catch (err) {
		console.error('https support is disabled!');
	}
	
	// Use the axios module for REST
	
	// A direct read, without passing  through a query.

	console.log(protocol + '://' + host + ':' + port + db + uri);
	try {
		axios.get(protocol + '://' + host + ':' + port + '/infinitydb/data' + db + uri, {
				auth: { username: 'testUser', password: 'db' },
				responseType: 'arraybuffer' 
			}
		).then(function(response) {
			const blob = new IdbBlob(response.data, response.headers['content-type']);
			console.log('From axios: ' + blob.contentType + ' blob length ' + blob.v.length);
			testReceivedData(blob);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}

	// A direct write. This requires write permission, with no query involved.
	// Actually axios can do the text encoding too, but we show optional IdbBlobs.
	// In fact axios handles JSON conveniently also. We write a single value,
	// on a prefix ending in a date, so we see a 'log' of the writes.
	
	const encoder = new TextEncoder();
	let encoded = encoder.encode('{ "hello from axios" : null }');
	const blobToPost = new IdbBlob(encoded, 'application/json'); 
	try {
		axios.post(protocol + '://' + host + ':' + port + '/infinitydb/data' 
				+ db + trashJavaScriptDemoUri + '/DemoPost/' 
				+ new Date().toISOString() + '?action=write',
					blobToPost.v, {
				headers: { 'Content-Type' : blobToPost.contentType },
				auth: { username: 'testUser', password: 'db' },
//				responseType: 'arraybuffer' 
			}
		).then(function(response) {
//			const blob = new IdbBlob(response.data, response.headers['content-type']);
			console.log('to axios: ' + blobToPost.contentType + ' blobToPost length ' + blobToPost.v.length);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}

	// Use put to merge rather than post to copy. This does not
	// clear the prefix first like post does, but merges new Items with pre-existing
	// Items. There is also DELETE mode that clears the prefix.
	
	encoded = encoder.encode('{ "_DemoPost" : { "_' + new Date().toISOString() 
		+ '" : { "hello from axios" : null }}}');
	const blobToPut = new IdbBlob(encoded, 'application/json'); 
	try {
		axios.put(protocol + '://' + host + ':' + port + '/infinitydb/data' 
				+ db + trashJavaScriptDemoUri + '?action=write',
					blobToPut.v, {
				headers: { 'Content-Type' : blobToPut.contentType },
				auth: { username: 'testUser', password: 'db' },
//				responseType: 'arraybuffer' 
			}
		).then(function(response) {
//			const blob = new IdbBlob(response.data, response.headers['content-type']);
			console.log('to axios: ' + blobToPut.contentType + ' blobToPut length ' + blobToPost.v.length);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}
	
	// Execute a query that takes a name and returns a blob with an image.
	// We do execute-get blob-query so that the response will be raw binary 
	// like image/jpeg, rather than json.

	const blobRequest = new IdbBlob(encoder.encode('{ "_name": "pic0" }'), 'application/json');
	try {
		axios.post(protocol + '://' + host + ':' + port + '/infinitydb/data' 
				+ db + '/"examples"/"Display Image"?action=execute-get-blob-query',
					blobRequest.v, {
				headers: { 'Content-Type': 'application/json' },
				auth: { username: 'testUser', password: 'db' },
				responseType: 'arraybuffer'
			}
		).then(function(response) {
			const blobResponse = new IdbBlob(response.data, response.headers['content-type']);
			console.log('query axios: ' + blobResponse.contentType + ' queryResponse length ' + blobResponse.v.length);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}

	// Use the https module	for REST


	const options = {
		hostname: host,
		path: '/infinitydb/data' + db + uri,
		port: port,
		method: 'GET',
		headers: {
			'Authorization': 'Basic ' + Buffer.from('testUser:db').toString('base64')
		}
	};

	// Remember to switch this to correspond to protocol above
	
	const req = https.request(options, (res) => {
//	const req = http.request(options, (res) => {
		// Return raw data.
		const dataChunks = [];

		res.on('data', (chunk) => {
			dataChunks.push(chunk);
		});
		res.on('end', () => {
			const fullData = Buffer.concat(dataChunks);
			const blob = new IdbBlob(fullData, res.headers['content-type']);
			console.log('From https: ' + blob.contentType + ' blob length ' + blob.v.length);
			testReceivedData(blob);
		});
	});
	req.on('error', (error) => {
		console.error(error);
	});
	req.end();

}

main();
