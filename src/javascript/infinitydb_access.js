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
// will forget.) So, we provide optional idb.FLoat, idb.Long, and 
// idb.Double to hold a number that will retain its InfinityDB meaning.


// All of this code is really optional and is provided in the
// hope it will be useful. You can do everything by hand.
// Parsing IdbBlobs is pretty useful.

let axios;
if (typeof window == "undefined") {
    // If running in nodeJS, use axios instead of the built-in
    // fetch() API. There is a fetch() implementation in nodeJS, but 
    // it doesn't seem to send the JSON correctly. 
    axios = await import('axios').then((x) => x.default);
}

// Import this namespace in client javascript
export const idb = {}

// Ordering of component types in InfinityDB for sorting
idb.TYPE_ORDER = {
    CLASS: 1,
    ATTRIBUTE: 2,
    STRING: 3,
    BOOLEAN: 4,
    FLOAT: 5,
    DOUBLE: 6,
    LONG: 7,
    DATE: 8,
    BYTES: 9,
    BYTE_STRING: 10,
    CHARS: 11,
    INDEX: 12
};

// Item - a sequence of components that can be atomically stored in the database.
// It is a path of components, and the last component is the value.
idb.Item = class IdbItem {
    constructor(...components) {
        this.components = [];

        // Add initial components
        for (const component of components) {
            this.addComponent(component);
        }
    }

    // Items are not commonly needed to be as JSON. Nornally,
    // they are used to create a path for a URI or to be stored in the database.
    toJSON() {
        return JSON.stringify(this.components.map(c => idb.key(c)));
    }

    // Add a component to the item
    addComponent(component) {
        if (component === null || component === undefined) {
            throw new TypeError('Cannot add null or undefined component');
        }

        // Auto-wrap primitive values
        if (!idb.Component.isComponent(component)) {
            component = idb.wrapValue(component);
        }

        this.components.push(component);
        return this;
    }

    // Get all components
    getComponents() {
        return [...this.components];
    }

    // Get a component at a specific index
    getComponent(index) {
        return this.components[index];
    }

    // Get the number of components
    getLength() {
        return this.components.length;
    }

    // Convert to array of JavaScript values. This loses the component types.
    // For example, an idb.FLoat() which is 32 bits beomes a number and can no longer be
    // distinguished from a 64-bit double, and an idb.Double which is 64 bits
    // that happens to be an exact integer (like 5.0) becomes a number
    // and will be translated back to
    // an idb.Long() when it is stored in the database.
    // So this is lossy.

    toArray() {
        return this.components.map(component =>
            component instanceof idb.Component ? component.v : component
        );
    }

    // Convert to token string representation
    toString() {
        return this.components.map(component => idb.toToken(component)).join(' ');
    }

    // Create a path for URI encoding
    toUriPath() {
        return idb.encodeUri(...this.toArray());
    }

    // Convert to a nested object structure for IDB
    toIdbObject() {
        if (this.components.length === 0) {
            return {};
        }

        let obj = {};
        let current = obj;

        // Build nested structure
        for (let i = 0; i < this.components.length - 1; i++) {
            const comp = this.components[i];
            const key = idb.key(comp);
            current[key] = {};
            current = current[key];
        }

        // Set the leaf value
        const lastComp = this.components[this.components.length - 1];
        const lastKey = idb.key(lastComp);
        current[lastKey] = null;

        return obj;
    }

    // Create a partial item by taking components from start to end (inclusive)
    slice(start, end) {
        if (end === undefined) {
            end = this.components.length - 1;
        }

        const newItem = new idb.Item();
        for (let i = start; i <= end && i < this.components.length; i++) {
            newItem.addComponent(this.components[i]);
        }

        return newItem;
    }

    // Create from an IDB object key path like 
    // { '_meta': { 'str': { '__str': { '_5' : null } } } }
    static fromKeyPath(obj) {
        const item = new idb.Item();

        let current = obj;
        const keys = [];

        // Extract keys from nested structure
        while (current !== null && typeof current === 'object') {
            const objKeys = Object.keys(current);
            if (objKeys.length === 0) {
                break;
            }

            const key = objKeys[0];
            keys.push(key);
            current = current[key];
        }

        // Create components from keys
        for (const key of keys) {
            item.addComponent(idb.unQuote(key));
        }

        return item;
    }

    // Compare this item with another item
    compareTo(other) {
        if (!(other instanceof idb.Item)) {
            throw new TypeError('Can only compare with another Item');
        }

        const len1 = this.getLength();
        const len2 = other.getLength();
        const minLen = Math.min(len1, len2);

        // Compare component by component
        for (let i = 0; i < minLen; i++) {
            const comp1 = this.getComponent(i);
            const comp2 = other.getComponent(i);

            // Use component's compareTo method if available
            if (comp1.compareTo) {
                const result = comp1.compareTo(comp2);
                if (result !== 0) return result;
            } else {
                // Fall back to string comparison
                const str1 = String(comp1);
                const str2 = String(comp2);
                const result = str1.localeCompare(str2);
                if (result !== 0) return result;
            }
        }

        // If all components match up to the minimum length, shorter item comes first
        return len1 - len2;
    }
}

idb.Component = class IdbComponent {

    // v always has the value of any subclass.

    constructor(v) {
        this.v = v;
    }

    toJSON() {
        return '"' + idb.key(toString()) + '"';
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
                || o instanceof idb.Component);
    }

    // 'impossible'. overriden in subclasses.
    getTypeOrder() {
        return idb.TYPE_ORDER.STRING; // Default type order
    }

    // Generic comparison method
    compareTo(other) {
        if (!(other instanceof idb.Component) &&
            !idb.Component.isComponent(other)) {
            throw new TypeError('Can only compare with another Component');
        }

        const thisType = this.getTypeOrder();
        const otherType = other instanceof idb.Component ?
            other.getTypeOrder() :
            idb._getTypeOrderForValue(other);

        // First compare by type order
        if (thisType !== otherType) {
            return thisType - otherType;
        }

        // If same type, compare values
        // Handle comparison with primitive values
        const thisValue = this.v;
        const otherValue = other instanceof idb.Component ? other.v : other;

        // Type-specific comparisons
        if (thisValue instanceof Date && otherValue instanceof Date) {
            return thisValue.getTime() - otherValue.getTime();
        }

        if (typeof thisValue === 'number' && typeof otherValue === 'number') {
            return thisValue - otherValue;
        }

        if (thisValue instanceof Uint8Array && otherValue instanceof Uint8Array) {
            // Compare byte by byte
            const len = Math.min(thisValue.length, otherValue.length);
            for (let i = 0; i < len; i++) {
                const diff = thisValue[i] - otherValue[i];
                if (diff !== 0) return diff;
            }
            return thisValue.length - otherValue.length;
        }

        // Default to string comparison
        return String(thisValue).localeCompare(String(otherValue));
    }
}

// Helper function to determine type order for primitive values
idb._getTypeOrderForValue = function (value) {
    if (value === null || value === undefined) {
        throw new TypeError('Cannot determine type order for null or undefined');
    }
    if (value instanceof idb.Component) {
        return value.getTypeOrder();
    }

    if (typeof value === 'string') {
        // It's always a string if it's not a component - never a meta
        return idb.TYPE_ORDER.STRING;
    }

    // This is not really recommended, because we should always use the
    // idb.Number subclasses, which preserve the true ordering
    // observed in the database. We lose the distinction between
    // idb.Float, idb.Double, and idb.Long.
    if (typeof value === 'number') {
        // Don't do this - it ruins the ordering.
        //   if (Number.isInteger(value)) {
        //     return idb.TYPE_ORDER.LONG;
        //   }
        return idb.TYPE_ORDER.DOUBLE;
    }

    if (typeof value === 'boolean') {
        return idb.TYPE_ORDER.BOOLEAN;
    }

    if (value instanceof Date) {
        return idb.TYPE_ORDER.DATE;
    }

    if (value instanceof Uint8Array) {
        return idb.TYPE_ORDER.BYTES;
    }

    throw new TypeError('Expected primitive value but was ' + value);
    // if (typeof value === 'object' || typeof value === 'function' || Array.isArray(value)) {	
    // 	throw new TypeError('Expected primitive value but was ' + value);
    // }

    // // Default. Not really expected to get here.
    // return idb.TYPE_ORDER.STRING;
}

// All but IdbMetas, string, number, boolean and Date.
// Numbers can either be primitives or subclasses of IdbNumber.
// Inside the DB, this includes all but Metas. 

idb.Primitive = class IdbPrimitive extends idb.Component {
    constructor(v) {
        super(v);
    }
}

// The Metas are the non-primitive data types and delimit the 'data'
// parts.

idb.Meta = class IdbMeta extends idb.Component {
    constructor(name) {
        super(name);
    }

    toString() {
        return this.v;
    }
}

// Instead of these, you can also write { _MyClass : 5; }

idb.Class = class IdbClass extends idb.Meta {
    constructor(name) {
        super(name);
        if (!idb.Class.isValidName(name)) {
            throw new TypeError('IdbClasses must be UC, then letters, digits, dots, and underscores');
        }
    }

    static isValidName(s) {
        const metaRegex = /^[A-Z][a-zA-Z\d\._]*$/;
        return metaRegex.test(s);
    }
    getTypeOrder() {
        return idb.TYPE_ORDER.CLASS;
    }

    // Classes compare first by name
    compareTo(other) {

        // If other is not a Class, first compare by type order
        if (!(other instanceof idb.Class)) {
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        // Both are Classes, compare by name
        return this.v.localeCompare(other.v);
    }
}

// Instead of these, you can also write { _myAttribute : 5; }

idb.Attribute = class IdbAttribute extends idb.Meta {
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
    getTypeOrder() {
        return idb.TYPE_ORDER.ATTRIBUTE;
    }

    // Attributes compare first by name
    compareTo(other) {
        // If other is not an Attribute, first compare by type order
        if (!(other instanceof idb.Attribute)) {
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        // Both are Attributes, compare by name
        return this.v.localeCompare(other.v);
    }
}

// Dates are handled separately even though they are InfinityDB
// 'primitive' types.

idb.isValidIsoDate = function (dateString) {
    const isoDateRegex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?)(Z|[-+]\d{2}:\d{2})?$/;
    return isoDateRegex.test(dateString);
}

// Instead of this you can also write '_[n]' like '_[55]' or '_Index(55)'. 
// An Index is one of 12 InfinityDB types, and when it occurs in an Item,
// it indicates that the JSON form of it is an array. For example, in
// the token form of an Item, it might be:
// "abc" [55] myAttribute. 
// In a URL quoted by idbUriQuote it might be: 
// ['abc',new idb.Index(55),new IdbAttribute('myAttribute')],
// and in accessing a JS object, it would be x.abc[55]._myAttribute.

idb.Index = class IdbIndex extends idb.Primitive {
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

    getTypeOrder() {
        return idb.TYPE_ORDER.INDEX;
    }
    // Indices compare numerically
    compareTo(other) {
        if (!(other instanceof idb.Index)) {
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.Index ? other.v : other;
        return this.v - otherValue;
    }

    toString() {
        return '[' + this.v + ']';
    }
}

// We insist on caps
idb.hexChars = '0123456789ABCDEF';

// No corresponding InfinityDB type: use Bytes and ByteString.

idb.ByteArray = class IdbByteArray extends idb.Primitive {

    // protected access
    constructor(bytes, name) {
        super(bytes);
        this.name = name;
        if (bytes == null) {
            this.v = new Uint8Array(0);
        } else if (bytes instanceof Uint8Array) {
            this.v = bytes;
        } else {
            throw new TypeError('expected Uint8Array: ' + bytes);
        }
    }
    parse(o) {
        if (typeof o === 'string') {
            if (!o.startsWith(this.name + '(') || !o.endsWith(')')) {
                throw new TypeError('expected "' + this.name + '(...) " but was ' + o);
            }
            this.v = idb.ByteArray.fromHexWithUnderscores(o.slice(this.name.length + 1, -1));
        } else {
            throw new TypeError('expected string: ' + o);
        }
        return this;
    }

    toString() {
        return this.name + '(' + idb.ByteArray.toHexWithUnderscores(this.v) + ')';
    }

    toJSON() {
        return '"_' + this.name + '(' + idb.ByteArray.toHexWithUnderscores(this.v) + ')"';
    }

    // Convert 'A6_99' to Uint8Array([0xa6,99]) for Bytes and ByteString

    static fromHexWithUnderscores(s) {
        if (s.length == 0) {
            return new Uint8Array();
        }
        if ((s.length + 1) % 3 !== 0)
            throw new TypeError('Hex has an invalid length - must be 3*n - 1: ' +
                'length=' + s.length + ' o=' + s);

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

            const hexValue = idb.hexChars[(byte >> 4) & 0xf] + idb.hexChars[byte & 0xf];

            if (!isFirst) {
                s += '_';
            }
            isFirst = false;

            s += hexValue;
        }
        return s;
    }
}

idb.ByteString = class IdbByteString extends idb.ByteArray {
    constructor(o) {
        super(o, 'ByteString');
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.BYTE_STRING;
    }

    // ByteStrings compare byte by byte
    compareTo(other) {
        if (!(other instanceof idb.ByteString)) {
            // If other is not ByteString, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherBytes = other instanceof idb.ByteArray ? other.v : other;

        // Compare byte by byte
        const len = Math.min(this.v.length, otherBytes.length);
        for (let i = 0; i < len; i++) {
            const diff = this.v[i] - otherBytes[i];
            if (diff !== 0) return diff;
        }

        // If all bytes match up to the minimum length, shorter array comes first
        return this.v.length - otherBytes.length;
    }
}

// Bytes compare by length first, then contents
idb.Bytes = class IdbBytes extends idb.ByteArray {
    constructor(o) {
        super(o, 'Bytes');
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.BYTES;
    }

    // Bytes compare by length first, then contents for speed
    compareTo(other) {
        if (!(other instanceof idb.Bytes) &&
            !(other instanceof Uint8Array)) {
            // If other is not Bytes, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherBytes = other instanceof idb.ByteArray ? other.v : other;

        // First compare by length for speed
        const lenDiff = this.v.length - otherBytes.length;
        if (lenDiff !== 0) return lenDiff;

        // If same length, compare byte by byte
        for (let i = 0; i < this.v.length; i++) {
            const diff = this.v[i] - otherBytes[i];
            if (diff !== 0) return diff;
        }

        return 0; // Equal
    }
}

// Chars compare by length first, then contents
idb.Chars = class IdbChars extends idb.Primitive {
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
            throw new TypeError('expected string or Uint8Array but was ' + s);
        }
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.CHARS;
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

    // Chars compare by length first, then string value for speed
    compareTo(other) {
        if (!(other instanceof idb.Chars)) {
            // If other is not Chars, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.Chars ? other.v : other;

        // First compare by length for speed
        const lenDiff = this.v.length - otherValue.length;
        if (lenDiff !== 0) return lenDiff;

        // If same length, compare by string value
        return this.v.localeCompare(otherValue);
    }
}

idb.BooleanValue = class IdbBooleanValue extends idb.Primitive {
    constructor(value) {
        super(Boolean(value));
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.BOOLEAN;
    }

    toString() {
        return this.v.toString();
    }

    // Booleans compare with false < true
    compareTo(other) {
        if (!(other instanceof idb.BooleanValue) &&
            !(typeof other === 'boolean')) {
            // If other is not a Boolean, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.BooleanValue ? other.v : other;
        return this.v === otherValue ? 0 : (this.v ? 1 : -1);
    }
}

idb.Date = class IdbDate extends idb.Primitive {
    constructor(value) {
        if (!(value instanceof Date)) {
            value = new Date(value);
        }
        super(value);
        if (isNaN(this.v.getTime())) {
            throw new TypeError('Invalid date: ' + value);
        }
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.DATE;
    }

    getTimestamp() {
        return this.v.getTime();
    }

    toISOString() {
        return this.v.toISOString();
    }

    toString() {
        return this.v.toISOString();
    }

    // Dates compare by timestamp
    compareTo(other) {
        if (!(other instanceof idb.DateValue) &&
            !(other instanceof Date)) {
            // If other is not a Date, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.DateValue ? other.v : other;
        return this.v.getTime() - otherValue.getTime();
    }
}

// There is corresponding type within the db: use idb.Long, idb.Double, or idb.Float
// subclasses.

idb.Number = class IdbNumber extends idb.Primitive {
    constructor(n) {
        super(n);
    }

    static isDigit(o) {
        return typeof o === 'string' && '0123456789'.includes(o.charAt(0));
    }
}

idb.Double = class IdbDouble extends idb.Number {
    constructor(n) {
        super(n);
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.DOUBLE;
    }

    // This format is how InfinityDB identifies a double: always a decimal
    // point. Normal JavaScript numbers are interpreted as doubles.
    toString() {
        return Number.isInteger(this.v) ? this.v + '.0' : this.v.toString();
    }

    // Doubles compare by numeric value
    compareTo(other) {
        if (!(other instanceof idb.Double)) {
            // If other is not a Double, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.Double ? other.v : other;
        return this.v - otherValue;
    }
}

idb.Float = class IdbFloat extends idb.Number {
    constructor(n) {
        super(n);
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.FLOAT;
    }

    // This format is how InfinityDB identifies a float: always a 
    // decimal point and 'f'.
    toString() {
        return (Number.isInteger(this.v) ? this.v + '.0' : this.v.toString()) + 'f';
    }

    // Floats compare by numeric value
    compareTo(other) {
        if (!(other instanceof idb.Float)) {
            // If other is not a Float, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.Float ? other.v : other;
        return this.v - otherValue;
    }
}

idb.Long = class IdbLong extends idb.Number {
    constructor(n) {
        super(n);
        if (!Number.isInteger(n)) {
            throw new TypeError('Expected integer for Long(n) but was ' + n);
        }
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.LONG;
    }

    // This format is how InfinityDB distinguishes a long: no decimal point.
    toString() {
        return this.v.toString();
    }

    // Longs compare by numeric value
    compareTo(other) {
        if (!(other instanceof idb.Long)) {
            // If other is not a Long, first compare by type order
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.Long ? other.v : other;
        return this.v - otherValue;
    }
}

idb.String = class IdbString extends idb.Component {
    constructor(s) {
        super(s);
        if (s == null) {
            n = '';
        } else if (typeof n !== 'string') {
            throw new TypeError('Expected string for String(n) but was ' + n);
        }
    }

    getTypeOrder() {
        return idb.TYPE_ORDER.STRING;
    }

    toString() {
        return JSON.stringify(this.v);
    }

    toJSON() {
        return JSON.stringify(idb.key(this.v));
    }

    compareTo(other) {
        if (!(other instanceof idb.String) &&
            !(typeof other === 'string')) {
            return this.getTypeOrder() - idb._getTypeOrderForValue(other);
        }

        const otherValue = other instanceof idb.String ? other.v : other;
        return this.v - otherValue;
    }
}

// Helper function to wrap a JavaScript value with the appropriate IDB component
idb.wrapValue = function (value) {
    if (value === null || value === undefined) {
        throw new TypeError('Cannot wrap null or undefined');
    }

    // Idempotent: if it's already a component, return it as is
    if (idb.Component.isComponent(value)) {
        return value;
    }

    if (typeof value === 'string') {
        return new idb.StringValue(value);
    }

    // Unfortunately, JSON cannot represent a float, so we have to
    // use a double. InfinityDB has a float type that is 32 bits.
    // Also, we can't convert integers to idb.Long, because that would
    // ruin the sorting order. So we have to use a double for that too.
    // The problem is that InfinityDB keeps the float, double, and long
    // separate as distinct data types, and sorts them separately.
    // Try to keep the values as idb.Long() and idb.Float() for as long as possible.
    // But if you want to use them as numbers, you can do that too.
    if (typeof value === 'number') {
        // Don't do this.
        //   if (Number.isInteger(value)) {
        //     return new idb.Long(value);
        //   }
        return new idb.Double(value);
    }

    if (typeof value === 'boolean') {
        return new idb.BooleanValue(value);
    }

    if (value instanceof Date) {
        return new idb.Date(value);
    }

    if (value instanceof Uint8Array) {
        return new idb.Bytes(value);
    }

    throw new TypeError('Cannot wrap value: ' + value);
}

// OrderedMap that uses binary search to maintain sorted order
idb.OrderedMap = class IdbOrderedMap {
    constructor() {
        this.entries = []; // Array to store key-value pairs
        this.isLeafNode = false;
        this.item = null; // The complete item if this is a leaf node
    }

    // Binary search to find the index of a key or where it should be inserted
    _binarySearch(key) {
        let low = 0;
        let high = this.entries.length - 1;

        while (low <= high) {
            const mid = Math.floor((low + high) / 2);
            const midKey = this.entries[mid].key;

            // Use component compareTo method when available
            let comparison;
            if (midKey instanceof idb.Component && midKey.compareTo) {
                comparison = midKey.compareTo(key);
            } else if (key instanceof idb.Component && key.compareTo) {
                comparison = -key.compareTo(midKey);
            } else {
                // Fall back to string comparison
                comparison = String(midKey).localeCompare(String(key));
            }

            if (comparison === 0) {
                return { found: true, index: mid };
            } else if (comparison < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return { found: false, index: low }; // Return the insertion point
    }

    // Set a key-value pair
    set(key, value) {
        const { found, index } = this._binarySearch(key);
        if (found) {
            // Key already exists, update the value
            this.entries[index].value = value;
        } else {
            // Insert the new key-value pair at the correct position
            this.entries.splice(index, 0, { key, value });
        }
        return this;
    }

    // Get the value associated with a key
    get(key) {
        const { found, index } = this._binarySearch(key);
        if (found) {
            return this.entries[index].value;
        }
        return null; // Key not found
    }

    // Check if a key exists
    has(key) {
        const { found } = this._binarySearch(key);
        return found;
    }

    // Delete a key-value pair
    delete(key) {
        const { found, index } = this._binarySearch(key);
        if (found) {
            this.entries.splice(index, 1);
            return true;
        }
        return false; // Key not found
    }

    // Check if this node is empty (has no entries)
    isEmpty() {
        return this.entries.length === 0;
    }

    // Set whether this node is a leaf node
    setIsLeaf(isLeaf) {
        this.isLeafNode = isLeaf;
        if (!isLeaf) {
            this.item = null; // Clear item if not a leaf
        }
        return this;
    }

    // Check if this node is a leaf node
    isLeaf() {
        return this.isLeafNode;
    }

    // Get the number of entries
    size() {
        return this.entries.length;
    }

    // Iterate over the map in sorted order
    *[Symbol.iterator]() {
        for (const entry of this.entries) {
            yield [entry.key, entry.value];
        }
    }

    // Get all keys in sorted order
    keys() {
        return this.entries.map(entry => entry.key);
    }

    // Get all values in sorted order
    values() {
        return this.entries.map(entry => entry.value);
    }

    // Get all entries in sorted order
    entries() {
        return this.entries.map(entry => [entry.key, entry.value]);
    }

    // Convert to a standard object for debugging
    toObject() {
        const obj = {};

        for (const { key, value } of this.entries) {
            const keyRepr = key instanceof idb.Component ? key.toString() : String(key);

            if (value instanceof idb.OrderedMap) {
                if (value.isLeaf() && value.item) {
                    obj[keyRepr] = { _leaf: true, _item: value.item.toString() };
                } else if (value.isEmpty()) {
                    obj[keyRepr] = null;
                } else {
                    obj[keyRepr] = value.toObject();
                }
            } else {
                obj[keyRepr] = value;
            }
        }

        return obj;
    }
}

// ItemSpace with OrderedMap nodes for O(log n) operations
idb.ItemSpace = class IdbItemSpace {
    constructor() {
        this.root = new idb.OrderedMap(); // Root of the map view
        this.itemCount = 0; // Keep track of total items for convenience
    }

    // Add an item
    addItem(item) {
        if (!(item instanceof idb.Item)) {
            throw new TypeError('Can only add Item objects to ItemSpace');
        }

        if (item.getLength() === 0) {
            return this; // Skip empty items
        }

        // Check if item already exists
        if (this._itemExists(item)) {
            return this; // Skip duplicate items
        }

        // Add to map view with the full item stored in the leaf node
        this._addToMapView(item);
        this.itemCount++;

        return this;
    }

    // Check if an item already exists in the map
    _itemExists(item) {
        if (item.getLength() === 0) return false;

        let current = this.root;

        // Navigate to where the item should be
        for (let i = 0; i < item.getLength(); i++) {
            const component = item.getComponent(i);

            if (!current.has(component)) {
                return false; // Path doesn't exist, item not found
            }

            current = current.get(component);
        }

        // If we reach a leaf node, the item exists
        return current.isLeaf();
    }

    // Add multiple items at once
    addItems(items) {
        for (const item of items) {
            this.addItem(item);
        }
        return this;
    }

    // Remove an item
    removeItem(item) {
        if (!(item instanceof idb.Item) || item.getLength() === 0) {
            return this;
        }

        // Create a stack to track the path
        const stack = [];
        let current = this.root;

        // Find the path
        for (let i = 0; i < item.getLength(); i++) {
            const component = item.getComponent(i);

            if (!current.has(component)) {
                return this; // Item doesn't exist
            }

            stack.push({ node: current, key: component });
            current = current.get(component);
        }

        // If found, unmark as leaf and remove the stored item
        if (current.isLeaf()) {
            current.setIsLeaf(false);
            current.item = null;
            this.itemCount--;

            // Clean up empty nodes, from bottom to top
            if (current.isEmpty()) {
                for (let i = stack.length - 1; i >= 0; i--) {
                    const { node, key } = stack[i];

                    const child = node.get(key);
                    if (child.isEmpty() && !child.isLeaf()) {
                        node.delete(key);
                    } else {
                        break;
                    }
                }
            }
        }

        return this;
    }

    // Get all items by traversing the map
    getItems() {
        const items = [];
        this._collectItems(this.root, items);
        return items;
    }

    // Helper to recursively collect all items
    _collectItems(node, items) {
        if (node.isLeaf()) {
            items.push(node.item);
        }

        for (const [, childNode] of node) {
            this._collectItems(childNode, items);
        }
    }

    // Get the count of items
    count() {
        return this.itemCount;
    }

    // Convert all items to an IDB object and merge them
    toIdbObject() {
        // Create the merged object directly from the map structure
        return this._nodeToIdbObject(this.root);
    }

    // Convert a node subtree to an IDB object
    _nodeToIdbObject(node) {
        if (node.isLeaf() && node.isEmpty()) {
            return null;
        }

        const obj = {};

        if (node.isLeaf()) {
            // Leaf nodes with an item should create their object representation
            return node.item.toIdbObject();
        }

        // Process all entries
        for (const [key, childNode] of node) {
            const keyString = idb.key(key);
            const childObj = this._nodeToIdbObject(childNode);

            if (childObj !== null) {
                // Merge the child object into this level
                idb.ItemSpace._deepMerge(obj, { [keyString]: childObj });
            } else {
                // Handle leaf nodes with no children
                obj[keyString] = null;
            }
        }

        return obj;
    }

    // Helper method to merge objects deeply
    static _deepMerge(target, source) {
        for (const key in source) {
            if (source.hasOwnProperty(key)) {
                if (source[key] === null) {
                    // For leaf nodes
                    target[key] = null;
                } else if (typeof source[key] === 'object') {
                    // For nested objects
                    if (!target[key]) {
                        target[key] = {};
                    }
                    idb.ItemSpace._deepMerge(target[key], source[key]);
                } else {
                    target[key] = source[key];
                }
            }
        }
        return target;
    }

    // Create an ItemSpace from an IDB object
    static fromIdbObject(obj) {
        const space = new idb.ItemSpace();
        const paths = [];

        // Recursively extract all paths
        function extractPaths(current, path = []) {
            if (current === null) {
                paths.push([...path]);
                return;
            }

            if (typeof current !== 'object') {
                return;
            }

            for (const key in current) {
                if (current.hasOwnProperty(key)) {
                    path.push(key);
                    extractPaths(current[key], path);
                    path.pop();
                }
            }
        }

        extractPaths(obj);

        // Convert paths to items
        for (const path of paths) {
            const item = new idb.Item();
            for (const key of path) {
                item.addComponent(idb.unQuote(key));
            }

            // Add to this space
            space.addItem(item);
        }

        return space;
    }

    // === Map View Interface ===

    // Get the root map node
    getRoot() {
        return this.root;
    }

    // Get a submap with the given key
    get(key) {
        return this.root.has(key) ? this.root.get(key) : null;
    }

    // Check if a key exists in the root
    has(key) {
        return this.root.has(key);
    }

    // Get all keys at the root level (already sorted)
    keys() {
        return this.root.keys();
    }

    // Get all entries [key, submap] at the root level (already sorted)
    entries() {
        return this.root.entries();
    }

    // Iterate through all keys (already sorted)
    *[Symbol.iterator]() {
        for (const key of this.keys()) {
            yield key;
        }
    }

    // Internal method to add an item to the map view
    _addToMapView(item) {
        if (item.getLength() === 0) return;

        let current = this.root;

        // Build path in the map view
        for (let i = 0; i < item.getLength(); i++) {
            const component = item.getComponent(i);

            if (!current.has(component)) {
                current.set(component, new idb.OrderedMap());
            }

            current = current.get(component);
        }

        // Mark as a leaf node and store the complete item
        current.setIsLeaf(true);
        current.item = item;
    }
}


// Quote an object of the 12 data types to make it compatible with
// a string object key. All of the component types have an underscore
// placed in front except strings. A string that happens to already
// have an underscore is quoted by 'stuffing in' another one.

idb.key = function (o) {
    if (o === null || o === undefined) {
        throw new TypeError('keys must not be null');
    }
    if (typeof o === 'string') {
        return o.charAt(0) !== '_' ? o : '_' + o;
    }
    return '_' + idb.toToken(o);
}

// Quote something of the 12 data types to make it compatible with
// an object value. We don't change number into
// strings though, which means InfinityDB will interpret the
// number as a double! If you want to have the number underscore
// quoted, use idb.doubleKey(), idb.longKey(n) or idb.floatKey(n) 
// to generate '_5.0' '_5' or '_5.0f' to correspond to the types
// within InfinityDB. We also provide idb.Float, idb.Double, and
// idb.Long classes that remember their
// types, and you can get the contained number with myNumber.v.

idb.value = function (o) {
    if (o === undefined) {
        throw new TypeError('idb.value(undefined)');
    } else if (o === null || typeof o === 'boolean' || typeof o === 'number') {
        return o;
    } else {
        return idb.key(o);
    }
}

// Unquote an underscore-quoted key or value.
// Be careful with numbers: InfinityDB Long, Float and Double
// become the corresponding classes idb.Double, idb.Long or 
// idb.Float here , so you may have to use mylong.v. 
// Non-string JS primitives are just returned.

idb.unQuote = function (o) {
    if (o === undefined) {
        throw new TypeError('idb.unQuote(undefined)');
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
    return idb.fromToken(o.slice(1));
}

// The InfinityDB standard way to represent each data type as a string.

idb.toToken = function (o) {
    if (o === null || o === undefined) {
        throw new TypeError('There is no null data type in InfinityDB');
    } else if (typeof o === 'boolean') {
        return o ? 'true' : 'false';
    } else if (typeof o === 'number') {
        return new idb.Double(o).toString();
    } else if (typeof o === 'string') {
        return JSON.stringify(o);
    } else if (o instanceof Date) {
        return o.toISOString();
    } else if (o instanceof idb.Component) {
        return o.toString();
    } else if (o instanceof Uint8Array) {
        return new idb.Bytes(o).toString();
    } else {
        throw new TypeError('Uknown data type: cannot create token form of : ' + o);
    }
}

// Parse a standard InfinityDB token that represents a single
// component of the 12 data types, like Class, Attribute, 
// String, Long, Boolean, Date etc. When Items are printed by
// default they just come out as a sequence of these tokens.

// Note that the three numeric types come out as instances of idb.Long, idb.Double, and
// idb.Float. This way they preserve their types for later writing back.
// You can get the value with myfloat.v. Sometimes you will find regular
// JavaScript numbers, and these are interpreted as doubles, although
// longs are more common in the database, so be careful.

idb.fromToken = function (o) {
    if (o === 'null') {
        return null;
    } else if (o === 'true') {
        return true;
    } else if (o === 'false') {
        return false;
    } else if (idb.Number.isDigit(o.charAt(0))) {
        if (idb.isValidIsoDate(o)) {
            return new Date(o);
        }
        if (o.includes('.')) {
            if (o.endsWith('f')) {
                if (isNaN(o.slice(0, -1))) {
                    throw new TypeError("expected float but was " + o);
                }
                return new idb.Float(Number.parseFloat(o.slice(0, -1)));
            }
            if (isNaN(o)) {
                throw new TypeError("expected double but was " + o);
            }
            return new idb.Double(Number.parseFloat(o));
        } else {
            if (isNaN(o)) {
                throw new TypeError("expected long but was " + o);
            }
            return new idb.Long(Number.parseInt(o));
        }
    } else if (!o.includes('(')) {
        if (o.startsWith('[')) {
            return new idb.Index().parse(o);
        } else if (idb.upperCaseRegex.test(o.charAt(0))) {
            return new idb.Class(o);
        } else {
            return new idb.Attribute(o);
        }
    } else if (o.startsWith('Bytes(')) {
        return new idb.Bytes().parse(o);
    } else if (o.startsWith('ByteString(')) {
        return new idb.ByteString().parse(o);
    } else if (o.startsWith('Chars(')) {
        return new idb.Chars().parse(o);
    } else if (o.startsWith('Index(')) {
        return new idb.Index().parse(o);
    } else {
        throw new TypeError('cannot underscore-unquote value=' + o);
    }
}
idb.upperCaseRegex = /^[A-Z]$/;

idb.doubleKey = function (n) {
    return '_' + new idb.Double(n);
}

idb.floatKey = function (n) {
    return '_' + new idb.Float(n);
}

idb.longKey = function (n) {
    return '_' + new idb.Long(n);
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
// of idb.Bytes() which are 1024 long except the last, which is 
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
// The value can be a uint8array, a string, or a JSON object. The contentType is a string.
// This represents a blob of data, which is a sequence of bytes.
// The contentType is a string that describes the type of data in the blob.
// Blobs of type 'application/json' are parsed into JSON objects,
// and if the data is already a JSON object, it is not parsed again and
// must be of type 'application/json'
// Blobs of type 'text/*' are parsed into strings.

idb.Blob = class IdbBlob {

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
                    throw new TypeError('new idb.Blob() string data requires a' +
                        ' content type of "text/..." but was ' + contentType);
                }
            } else if (data instanceof Uint8Array) {
                // v already set
            } else if (data instanceof ArrayBuffer) {
                this.v = new Uint8Array(data);
            } else if (typeof data === 'object') {
                // The JSON is already -parsed for us.
                if (contentType !== 'application/json') {
                    throw new TypeError('new idb.Blob() object data requires a' +
                        ' content type of "application/json" but was ' + contentType);
                }
            } else {
                throw new TypeError('new idb.Blob() object data of incompatible type: ' + idb.printType(data));
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
            return JSON.parse(decoder.decode(this.v));
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
            throw new TypeError('Expected a blob structured object but was ' + idb.printType(o));
        }
        const blobInternals = o['_com.infinitydb.blob'];
        if (blobInternals == null) {
            return null;
        }
        if (typeof blobInternals !== 'object') {
            throw new TypeError('Expected a blob structured object but was' +
                ' missing attribute com.infinitydb.blob and was ' + idb.printType(blobInternals));
        }
        const contentType = blobInternals['_com.infinitydb.blob.mimeType'];
        if (typeof contentType !== 'string') {
            throw new TypeError('Expected a blob structured object but was missing' +
                ' attribute com.infinitydb.blob.mimeType: ' + idb.printType(contentType));
        }
        const array = blobInternals['_com.infinitydb.blob.bytes'];
        if (array == null || !Array.isArray(array)) {
            throw new TypeError('Expected a blob structured object but was' +
                ' missing attribute com.infinitydb.blob.bytes ' + idb.printType(array));
        }

        var theFullData = null;
        if (array.length === 0) {
            theFullData = new Uint8Array(0);
        } else {
            const chunks = [];
            for (const b of array) {
                const idbBytes = new idb.Bytes().parse(b.slice(1));
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
            const chunk = this.v.slice(i, i + 1024);
            bytesArray.push(new idb.Bytes(chunk).toJSON());
        }
        const structure = {
            '_com.infinitydb.blob': {
                '_com.infinitydb.blob.bytes': bytesArray,
                '_com.infinitydb.blob.mimeType': this.contentType
            }
        }
        return structure;
    }

    // static isBlob(o) {
    // 	if (o == null || typeof o !== 'object') {
    // 		return false;
    // 	}
    // 	if (o instanceof idb.Blob) {
    // 		return true;
    // 	}
    // 	const blobInternals = o['_com.infinitydb.blob'];
    // 	return blobInternals != null;
    // }

    toJSON() {
        //console.log(JSON.stringify(this.toBlobStructure()));
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
            return s;
        } else if (typeof this.v === 'object') {
            if (this.contentType !== 'application/json') {
                throw new TypeError('Found a blob containing an object' +
                    ' but is is not application/json: ' + this.contentType);
            }
            return JSON.stringify(this.v);
        } else {
            throw new TypeError('IdbBlob contains incompatible data of type ' +
                idb.printType(this.v) + ' content type is ' + this.contentType);
        }
    }

    toUint8Array() {
        if (typeof this.v === 'string') {
            if (!this.contentType.startsWith('text/') &&
                this.contentType !== 'application/json') {
                // We cannot convert a string to a Uint8Array unless it is UTF-8.
                throw new TypeError('IdbBlob contains a string but has incompatible content type ' + this.contentType);
            }
            return new TextEncoder('utf-8').encode(this.v);
        } else if (this.v instanceof Uint8Array) {
            return this.v;
        } else if (typeof this.v === 'object') {
            parseFromBlobStructure(this.v);
            return this.v;
        } else {
            throw new TypeError('IdbBlob contains incompatible data of type ' +
                idb.printType(this.v) + ' content type is ' + this.contentType);
        }
    }

    // From an object or array, recursively construct a new one but
    // with all possible idb.Blobs instead of their object structures.
    // Then you can get their Uint8Array forms directly, such as for
    // images.

    static convertAllContainedBlobStructures(o) {
        if (o === undefined) {
            throw new TypeError('Undefined object');
        }
        if (o === null || typeof o !== 'object' || o instanceof idb.Component) {
            return o;
        }
        if (Array.isArray(o)) {
            const converted = [];
            for (const e of o) {
                converted.push(idb.Blob.convertAllContainedBlobStructures(e));
            }
            return converted;
        }
        const isIdbBlob = o['_com.infinitydb.blob'] != null;
        if (isIdbBlob) {
            const idbBlob = new idb.Blob().parseFromBlobStructure(o);
            //			console.log('IdbBlob ' + idbBlob);
            console.log('IdbBlob len ' + idbBlob.v.length);
            return idbBlob;
        }
        const converted = {};
        for (const k in o) {
            if (!o.hasOwnProperty(k))
                continue;
            const v = o[k];
            converted[k] = idb.Blob.convertAllContainedBlobStructures(v);
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
//   * a subclass of idb.Component, which will be encoded properly.
// You can do this by hand without much trouble. 

idb.encodeUri = function (...components) {
    let s = '';
    // Flatten
    const componentsFlat = [].concat(components);
    for (const component of componentsFlat) {
        s += '/' + idb.encodeUriComponent(component);
    }
    return s;
}

// Using a JS number defaults to double, which may not be obvious.
// You can use the idb.Long, idb.Double, and idb.Float classes for clarity.
// The main point is to prevent uri-quoting the slashes. You can 
// do this by hand in many cases.

idb.encodeUriComponent = function (s) {
    if (s === null || s === undefined) {
        throw new TypeError("Cannot encode a uri of null");
    } else if (typeof s === 'string') {
        s = JSON.stringify(s);
    } else if (typeof s == 'number' || typeof s == 'boolean' || s instanceof idb.Component) {
        s = s.toString();
    }
    // The built-in function
    return encodeURIComponent(s);
}

// Get a string representing the type of any value
idb.printType = function (o) {
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

// This demonstrates recursing over a JSON tree in order to print
// a JSON tree. Not very useful, but it can print also in the
// InfinityDB extended JSON form, where we minimize quotes.
// It shows using the unquoter idbUnQuote() to get the key out,
// and the quoter idb.key() to re-create it ready for use as a key.
// Also, if we have already run idbConvertAllContainedBlobs, then
// the tree contains Uint8Arrays, and we want to avoid them
// printing literally - they convert back to the original form.

// indent is a string to concatenate depth times

idb.printAsJSON = function (o, depth, isExtended, indentString) {
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
    if (idb.Component.isComponent(o)) {
        return isExtended ? idb.toToken(idb.unQuote(o)) : JSON.stringify(o);
    } else if (o instanceof idb.Blob) {
        return idb.printAsJSON(o.toBlobStructure(), depth, isExtended, indentString);
    } else if (Array.isArray(o)) {
        s += '[\r\n';
        for (var e of o) {
            if (!isFirst) {
                s += ',\r\n';
            }
            isFirst = false;
            s += indent(depth + 1, indentString);
            if (e !== null) {
                s += idb.printAsJSON(e, depth + 1, isExtended, indentString);
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
            s += (isExtended ? idb.toToken(idb.unQuote(k)) : JSON.stringify(k)) + ': ';
            const v = o[k];
            s += idb.printAsJSON(v, depth + 1, isExtended, indentString);
        }
        s += '\r\n' + indent(depth, indentString) + '}';
    } else {

    }
    return s;
}

/*
Preprocess an object to use as the input data of a pattern query.
*/
idb.unflattenQueryData = function (obj) {
    if (obj == null) return null;
    let unflattened_obj = {};
    for (let [k, v] of Object.entries(obj)) {
        if (v instanceof Array) {
            unflattened_obj[k] = idb.unflattenList(v);
        }
        else {
            let leaf_obj = {};
            leaf_obj[idb.key(v)] = null;
            unflattened_obj[k] = leaf_obj;
        }

    }
    return unflattened_obj;

}

/*
 Convert a list of primitive types to a dictionary, for use as JSON data
in a pattern query.
input: [x1, x2, x3...]  output: {x1: {x2: {x3: ....null}}}
*/
idb.unflattenList = function (l) {
    if (l.length == 0) return null;
    let obj = {};
    let prefix = l[0];
    let suffix = l.slice(1);

    obj[idb.key(prefix)] = idb.unflattenList(suffix);
    return obj;
}

/*
 convert a list of lists to a dictionary. 
 input: [[x, a1, a2, a3...],[x, b1, b2, b3...], ...]
 output: {x: {a1: {a2: a3: ... null}, b1: {b2: {b3: ...null}}}}

 This is useful for pattern queries where the input is a list of items,
 each of which should trigger a query match. 
*/

idb.unflattenFromLists = function (l) {
    if (!(l instanceof Array)) {
        let result = {};
        result[idb.toToken(l)] = null;
        return result;
    }
    if (l.length == 0) return null;
    let obj = {};

    for (let x of l) {
        if (x instanceof Array) {
            if (x.length == 0) continue;
            let prefix = x[0];
            let suffix = x.slice(1);


            prefix = idb.key(prefix);
            if (!(prefix in obj) || obj[prefix] == null) obj[prefix] = [];
            obj[prefix].push(suffix);
        }
        else {
            obj[idb.key(x)] = null;
        }
    }

    for (let key of Object.keys(obj)) {
        if (obj[key] instanceof Array) {
            obj[key] = idb.unflattenFromLists(obj[key]);
        }
    }
    if (Object.keys(obj).length == 0) return null;
    return obj;
}


/* Convert a nested dictionary back to a list of lists. The reverse of
unflatten_from_lists(). Useful for unpacking the output of a pattern query that returns
multiple lists.
*/
idb.flattenToLists = function (obj) {
    if (obj == null) return [];
    if (typeof obj != "object") {
        return [idb.unQuote(obj)];
    }
    let l = [];
    for (let [k, v] of Object.entries(obj)) {
        k = idb.unQuote(k);
        if (v == null) {
            l.push(k);
            continue;
        }

        let suffixes = idb.flattenToLists(v);
        for (let suffix of suffixes) {
            l.push([k].concat(suffix));
        }
    }
    return l;
}

/* The interface for running queries in browser-based JavaScript. Example:
server = IdbAccessor("https://infinitydb.com:37411/infinitydb/data", "demo/writeable", "myUser", "myPassword");
*/
idb.Accessor = class IdbAccessor {
    constructor(server_url, db, username, password) {
        this.server_url = server_url
        this.db = db
        this.username = username
        this.password = password
        this.db_url = this.server_url + "/" + this.db

        this.auth = btoa(this.username + ":" + this.password)
    }
    async _do_request_axios(query_url, method, data, request_content_type, blob_response) {
        let success = false;
        let result = null;
        let response_content_type = null;
        let response;
        const options = {
            url: query_url,
            method: method,
            auth: { username: this.username, password: this.password },
        };
        if (data != null) {
            options["data"] = data;
            options["headers"] = {
                "Content-Type": request_content_type,
            };
        }
        if (blob_response) {
            options["responseType"] = "arraybuffer";
        }

        try {
            // The request is made using axios, which is a promise-based HTTP client for the browser and Node.js.
            response = await axios.request(options);
            response_content_type = response.headers.get("Content-Type");
            success = (response.status == 200);
            if (success) {
                if (blob_response) {
                    result = new idb.Blob(response.data, response_content_type);
                }
                else {
                    result = response.data;
                }
            }
            else {
                console.error("Axios request to InfinityDB server failed with error: " + response.statusText);
            }

        }
        catch (error) {
            console.error("Error occurred during Axios request to infinityDB Server");
            console.error("Error: " + error.message);
            throw (error);
        }
        return [success, result, response_content_type];
    }

    async _do_request_fetch(query_url, method, data, request_content_type, blob_response) {
        let success = false;
        let result = null;
        let response_content_type = null;
        const options = {
            headers: {
                "Authorization": "Basic" + this.auth,
            },
            method: method
        }
        if (data != null) {
            options['body'] = data;
            options['headers']['Content-Type'] = request_content_type;
        }
        let request = new Request(query_url, options);
        try {
            let response = await fetch(request);
            success = (response.status == 200);
            if (success) {
                response_content_type = response.headers.get("Content-Type");
                if (blob_response) {
                    let blob = await response.blob();
                    let data = await blob.arrayBuffer();
                    result = new idb.Blob(data, response_content_type);

                }
                else if (response_content_type == "application/json") {
                    result = await response.json();
                }
            }
            else {
                console.error("Fetch request failed with error: " + response.statusText);
            }

        }
        catch (error) {
            console.error("Error occurred while connecting to InfinityDB server " + this.server_url);
            throw (error);
        }
        return [success, result, response_content_type];
    }

    _do_request(query_url, method, data = null, request_content_type = null, blob_response = false) {
        if (typeof window == "undefined") {
            return this._do_request_axios(query_url, method, data, request_content_type, blob_response);
        }
        else {
            return this._do_request_fetch(query_url, method, data, request_content_type, blob_response);
        }
    }

    make_query_url(prefix) {
        prefix = prefix.map(idb.encodeUriComponent).join("/");
        let query_url = new URL(this.db_url + '/' + prefix);
        return query_url;
    }

    async execute_query(prefix, data) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "execute-query");

        let request_content_type = null;
        if (data !== null) {
            data = data instanceof idb.ItemSpace ? data.toIdbObject()
                : idb.unflattenQueryData(data);
            data = JSON.stringify(data);
            request_content_type = "application/json";
        }

        let [success, result, content_type] = await this._do_request(query_url, "POST", data, request_content_type, false);

        return [success, result, content_type];
    }

    // Directly get a blob using its URI in the database. For example 
    // get_blob([new idb.Class("Pictures"), "pic0"])
    async get_blob(prefix) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "get-blob");

        let [success, result, content_type] = await this._do_request(query_url, "GET", null, null, true);
        return [success, result, content_type];

    }

    async put_json(prefix, data) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "write");
        data = data instanceof idb.ItemSpace ? data.toIdbObject()
            : idb.unflattenQueryData(data);
        data = JSON.stringify(data);

        let [success, result, content_type] = await this._do_request(query_url, "PUT", data, "application/json", false);
        return success;
    }

    async get_json(prefix) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "as-json");

        let [success, result, content_type] = await this._do_request(query_url, "GET", null, null, false);
        return [success, result, content_type];

    }

    async put_blob(prefix, blob) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "put-blob");

        let [success, result, content_type] = await this._do_request(query_url, "POST", blob.v, blob.contentType, false);
        return success;
    }

    async delete(prefix) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "write");

        let [success, result, content_type] = await this._do_request(query_url, "DELETE", null, null, false);
        return success;
    }

    /* Execute a pattern query that returns a blob rather than a JSON dictionary.
    */
    async execute_get_blob_query(prefix, data) {
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "execute-get-blob-query");
        let request_content_type = null;
        if (data !== null) {
            data = data instanceof idb.ItemSpace ? data.toIdbObject()
                : idb.unflattenQueryData(data);
            data = JSON.stringify(data);
            request_content_type = "application/json";
        }
        return this._do_request(query_url, "POST", data, request_content_type, true);
    }

    /* Execute a pattern query that sends a blob. The params is a json object
    * that will be sent in the params url parameter of the request. 
    * The params should not be very long, or we will get a very long URL.
    * The blob is sent in the body as raw data, and is efficient and fast.
    * The content type of the blob is sent in the content type header.
    * We have to use this params parameter because the blob is not 
    * necessarily a JSON object. The params url parameter can be explicitly
    * matched in the PatternQuery as a symbol of kind 'params url parameter'
    *
    * It is possible to send blobs as application/json with
    * embedded blob structures, but that is not efficient, at least
    * until z-lib compression is implemented.
    * However, you can embed multiple such blob structures in a single
    * JSON object, and that is efficient in that it reduces the number of I/O
    * operations. Use convertAllContainedBlobStructures() to do this.
    */
    async execute_put_blob_query(prefix, idbBlob, params = {}) {
        if (idbBlob == null) {
            throw new TypeError("IdbBlob is null in execute_put_blob_query()");
        }
        if (!(idbBlob instanceof idb.Blob)) {
            throw new TypeError("IdbBlob is not an instance of idb.Blob in execute_put_blob_query()");
        }
        if (idbBlob.v == null) {
            throw new TypeError("IdbBlob has no data in execute_put_blob_query()");
        }
        if (idbBlob.contentType == null) {
            throw new TypeError("IdbBlob has no content type in execute_put_blob_query()");
        }
        let raw_data = idbBlob.toUint8Array();
        let query_url = this.make_query_url(prefix);
        query_url.searchParams.append("action", "execute-put-blob-query");
        if (params != {}) {
            params = params instanceof idb.ItemSpace ? params.toIdbObject()
                : idb.unflattenQueryData(params);
            params = JSON.stringify(params);
            query_url.searchParams.append("params", params);
        }
        let content_type = idbBlob.contentType;
        return this._do_request(query_url, "POST", raw_data, content_type, false);
    }

    async head() {
        let [success, result, content_type] = await this._do_request(this.db_url, "HEAD");
        return success;
    }

}

