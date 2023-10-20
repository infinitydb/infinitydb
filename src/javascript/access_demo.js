import {idb} from "./infinitydb_access.js";
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
	const C = new idb.Class('C_a.b');
	// classes start with LC, then any letters, digits, underscores or dots
	const a = new idb.Attribute('a_a.b');
	const n = 5;

	const object = {
		
		// Quote any type! This is actually all you need except for dealing with
		// the number ambiguity in which there are three InfinityDB number types.
		
		[idb.key(o)]: idb.value(o),
		
		// This forces purely ISO dates
		
		[idb.key(new Date())]: idb.value(new Date()),
		// The primitive always prints iso
		['_2023-09-17T21:24:35.653Z']: idb.value(new Date('2023-09-17T21:24:35.653Z')),
		['_2023-09-17T21:24:35.653-07:00']: idb.value(new Date('2023-09-17T21:24:35.653-07:00')),

		// Use idbLongKey() to add the '_' to force long, which is a 64-bit integer
		// on the InfinityDB side
		
		_5: '_5',
		[idb.longKey(n)]: idb.longKey(n),
		// Use idb.floatKey() to add the '_' and '.' and 'f' to force float on the InfinityDB side
		[idb.floatKey(5.0)]: '_5.0f',
		[idb.floatKey(n)]: idb.floatKey(n),
		// Use the default double for numbers.
		// You will forget that double is default, not long!!
		// Even if the value is integer, because there are three number types.
		'_5.0': 5,
		'_5.1': 5.1,
		'_5.1': '_5.1',
		[idb.key(n)]: n,
		// Here x is a string on the InfinityDB side, not an attribute or class
		'x': 'x',
		// here _C is a class without the '_' on the InfinityDB side
		'_C_.a': '_C_.a',
		// Here _a is an attribute without the '_' on the InfinityDB side
		'_a._a': '_a._a',
		// here __s is a string starting with single quote '_' on the InfinityDB side
		// s is unknown string automatically underscore quoted if necessary
		[idb.key(s)]: idb.value(s),
		// Unknown class or attribute
		[idb.key(C)]: idb.value(C),
		[idb.key(a)]: idb.value(a),
		[idb.key(true)]: true,
		[idb.key(false)]: false,
		[idb.key(new Uint8Array())]: '_Bytes()',
		[idb.key(new idb.Bytes(new Uint8Array([1,0xa6])))]: '_Bytes(01_A6)',
		[idb.key(new idb.ByteString(new Uint8Array([1,0xa6])))]: '_ByteString(01_A6)',
		// Slight problem with backslashing backslashes..
		[idb.key(new idb.Chars('x\'\\\"'))]: '_Chars("x\'\\\"")',
		[idb.key(new idb.Index(5))]: '_[5]',
		[idb.key('subtree')] : {
			[idb.key(new idb.Double(3))]: 'double',
			[idb.key(6)]: 'double',
			[idb.key(6.0)]: 'double',
			[idb.key(6.1)]: 'double',
			[idb.key(new idb.Long(7))]: 'long',
			[idb.key(new idb.Float(3))]: 'float',
			[idb.key('null value')] : null
		}
	};
	return object;
}

function demonstrateUnQuoteObject(o) {
	for (const key in o) {
		const k = idb.unQuote(key);
		const v = idb.unQuote(o[key]);
		const tk = idb.printType(k);
		const tv = idb.printType(v);
		console.log(`Key: ${tk}:${k}, Value: ${tv}:${v}`);
	}
}


// THE REST INTERFACES

// There are quite a few such interfaces available to nodejs.
// See test.html also for how it is done in a browser with AJAX i.e. XHTML.

function main() {

	let date = new Date();
	console.log(date);
	date = idb.key(idb.toToken(date));
	console.log(date);
	date = idb.fromToken(date);
	console.log(date);

	// All of the code is really optional and is provided in the
	// hope it will be useful. You can do everything by hand, but
	// parsing idb.Blobs is pretty useful.

	// Demo the underscore-quoting functions

	const object = demonstrateCreateObject();
	console.log(JSON.stringify(object));
	demonstrateUnQuoteObject(object);

	// Demo the recursive descent access of an object
	// with underscore-quoted keys in order to handle the
	// 12 InfinityDB datatypes as key strings.
		
	console.log(idb.printAsJSON(object, 0, true));
	
	// Check the data received through the REST connection.
	
	function testReceivedData(blob) {
		checkConsistency(blob);
		// How to look inside the object if it is the documentation.
//		console.log('From https: ' + blob.getAsParsed().Basics._description[0]);
		if (blob.contentType == 'application/json') {
			// recursively replace all idb.Blobs from their structures in the
			// already parsed JSON.
			const o = blob.getAsParsed();
			const converted = idb.Blob.convertAllContainedBlobStructures(o);
			console.log('converted');
			const convertedPrinted = idb.printAsJSON(converted)
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
		const jsonPrinted = idb.printAsJSON(objectsParsed);
//		console.log('in check consistency: ' + jsonPrinted);
		// Go through another cycle
		const objectsReparsed = JSON.parse(jsonPrinted);
		const jsonPrinted2 = idb.printAsJSON(objectsReparsed);
		console.log(jsonPrinted !== jsonPrinted2 ? "inconsistent" : "consistent");
		return jsonPrinted;
	}

	// Some test URIs. Constant ones can alternatively just be url-quoted by hand
	
	// Read all of the queries as application/json. This is good exercize as all types are used.
	const queryUri = idb.encodeUri(new idb.Class('Query'));
	// The entire documentation as application/json
	const documentationsUri = idb.encodeUri(new idb.Class('Documentation'));
	// One picture, as binary image/jpeg - 1.7MB, fast, compressed.
	const picturesPic0Uri = idb.encodeUri(new idb.Class('Pictures'),'pic0');
	// All of the pictures as application/json - 35MB, not very compressed
	// These need to be parsed as multiple binary blobs out of their JSON form.
	const allPicturesUri = idb.encodeUri(new idb.Class('Pictures'));
	// Where we write things
	const trashJavaScriptDemoUri = idb.encodeUri(new idb.Class('Trash'),new idb.Class('JavaScriptDemo'));

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
			const blob = new idb.Blob(response.data, response.headers['content-type']);
			console.log('From axios: ' + blob.contentType + ' blob length ' + blob.v.length);
			testReceivedData(blob);
		}).catch(function(error) {
			console.error(error);
		});
	} catch (e) {
		console.log(e);
	}

	// A direct write. This requires write permission, with no query involved.
	// Actually axios can do the text encoding too, but we show optional idb.Blobs.
	// In fact axios handles JSON conveniently also. We write a single value,
	// on a prefix ending in a date, so we see a 'log' of the writes.
	
	const encoder = new TextEncoder();
	let encoded = encoder.encode('{ "hello from axios" : null }');
	const blobToPost = new idb.Blob(encoded, 'application/json'); 
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
//			const blob = new idb.Blob(response.data, response.headers['content-type']);
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
	const blobToPut = new idb.Blob(encoded, 'application/json'); 
	try {
		axios.put(protocol + '://' + host + ':' + port + '/infinitydb/data' 
				+ db + trashJavaScriptDemoUri + '?action=write',
					blobToPut.v, {
				headers: { 'Content-Type' : blobToPut.contentType },
				auth: { username: 'testUser', password: 'db' },
//				responseType: 'arraybuffer' 
			}
		).then(function(response) {
//			const blob = new idb.Blob(response.data, response.headers['content-type']);
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

	const blobRequest = new idb.Blob(encoder.encode('{ "_name": "pic0" }'), 'application/json');
	try {
		axios.post(protocol + '://' + host + ':' + port + '/infinitydb/data' 
				+ db + '/"examples"/"Display Image"?action=execute-get-blob-query',
					blobRequest.v, {
				headers: { 'Content-Type': 'application/json' },
				auth: { username: 'testUser', password: 'db' },
				responseType: 'arraybuffer'
			}
		).then(function(response) {
			const blobResponse = new idb.Blob(response.data, response.headers['content-type']);
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
			const blob = new idb.Blob(fullData, res.headers['content-type']);
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
