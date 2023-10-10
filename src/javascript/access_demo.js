import {idbKey, idbValue, 
	IdbChars, IdbDouble, IdbBytes, 
	IdbComponent, IdbBlob, IdbClass, IdbAttribute, idbLongKey,
	idbFloatKey, IdbByteString, IdbIndex, IdbLong, IdbFloat,
	idbUnQuote, idbPrintType, idbToToken, idbFromToken, idbEncodeUri} from "./infinitydb_access.js";
import axios from 'axios';
let https;
let http;
try {
    https = await import('node:https');
    http = await import('node:http');
} catch (err) {
    console.error('https support is disabled!');
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

	let date = new Date();
	console.log(date);
	date = idbKey(idbToToken(date));
	console.log(date);
	date = idbFromToken(date);
	console.log(date);

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
			const o = blob.getAsParsed();
			const converted = IdbBlob.idbConvertAllContainedBlobStructures(o);
			console.log('converted');
			const convertedPrinted = idbPrintAsJSON(converted)
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
