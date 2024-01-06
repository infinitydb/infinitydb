# Copyright (c) 2019-2021 Roger L. Deran
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the
# Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute,
# sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject
# to the following conditions:
#
# The above copyright notice and this permission notice
# shall be included in  all copies or substantial portions
# of the Software.
#  
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
# KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
# OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
# THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import datetime
import unittest
import re
import json
from infinitydb.access import *

class TestInfinityDB(unittest.TestCase):
    def test_parse_primitive(self):
        s = parse_primitive('"abc"')
        self.assertEqual(s,'abc')
        s = parse_primitive('5')
        self.assertEqual(s,5)
        s = parse_primitive('5.0')
        self.assertEqual(s,5.0)
        s = parse_primitive('5.0f')
        self.assertEqual(s,Float(5.0))
        s = parse_primitive('true')
        self.assertEqual(s,True)
        s = parse_primitive('false')    
        self.assertEqual(s,False)
        s = parse_primitive('null')
        self.assertEqual(s,None)
        s = parse_primitive('2019-1-1T0:0:1')
        self.assertEqual(s,datetime.datetime(2019, 1, 1, 0, 0, 1))
        s = parse_primitive('Bytes(5A_26)') 
        self.assertEqual(s,Bytes([0x5A,0x26]))
        s = parse_primitive('ByteString(5A_26)')
        self.assertEqual(s,ByteString([0x5A,0x26]))
        s = parse_primitive('Chars("abc")')
        self.assertEqual(s,Chars('abc'))
        s = parse_primitive('[3]')
        self.assertEqual(s,Index(3))        

        
        
    def test_constructors_and_comparisons(self):
        o = EntityClass('EntityClass3')
        o2 = EntityClass('EntityClass2')
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)

        o = Attribute('att3')
        o2 = Attribute('att2')
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)
        
        o = Bytes([0x5A,0x27])
        o2 = Bytes([0x5A,0x26])
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)

        o = ByteString([0x5A,0x27])
        o2 = ByteString([0x5A])
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)
        
        o = Chars('abc3\\"\\\\d"')
        o2 = Chars('abc3\\"\\\\d')
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)

        o = Float(1)
        o2 = Float(0)
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)

        o = Float(1)
        o2 = 0
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)

        o = 1
        o2 = Float(0)
        self.assertEqual(o, o)
        self.assertNotEqual(o, o2)
        self.assertGreater(o, o2)
        self.assertLess(o2, o)
        self.assertGreaterEqual(o, o2)
        self.assertLessEqual(o2, o)
        
        # we don't provide all possible implementations of math in Float
        # so Float(3) + Float(4) is not Float(7) but 7.0
        self.assertEqual(int(Float(5)),5)
        self.assertEqual(float(Float(5)),5)
        self.assertEqual(float(Float(5)) + 3, 8)
        self.assertEqual(3 + int(Float(5)), 8)
        self.assertEqual(Float(5.0) + 3, 8)
        self.assertEqual(Float(5.1) + 3, 8.1)
        self.assertEqual(Float(5.0) + 3, 8)

    def test_json_quote_primitive(self):
        self.assertEqual(json_quote_primitive('abc'), '"abc"')
        d = datetime.datetime(2019, 1, 1)
        self.assertEqual(json_quote_primitive(d), '"2019-01-01T00:00:00"')
        self.assertEqual(json_quote_primitive(5), '"5"')
        
    def test_json_quote_primitives(self):
        self.assertEqual(json_quote_primitives(
            { EntityClass('N'): 5, EntityClass('T'): True, "z" : { 3: datetime.datetime(2019, 5, 3)}}),
                         {'"N"':5,'"z"' : {'"3"':'"2019-05-03T00:00:00"'},'"T"':True})
        
    def test_escape_uri_components(self):
        components = ['abc',Float(6),5.0,3,Index(3),EntityClass('EC'),Attribute('a'),
                      Chars('x'),Bytes([0x5A,0x26]),ByteString([0x5A,0x26]),datetime.datetime(2019, 1, 1)]
        self.assertEqual(escape_uri_components(components),
                         '%22abc%22/6.0f/5.0/3/[3]/EC/a/Chars(%22x%22)/Bytes(5A_26)/ByteString(5A_26)/2019-01-01T00:00:00') 
    
    def test_parse_token_string_into_components(self):
        s = parse_token_string_into_components('E a "abc" true 5.0f 5.0 5 2019-01-01T00:00:00 Bytes(5A_26) ByteString(5A_26) Chars("x\\\\ \\"") [3]')
        self.assertEqual(s, [EntityClass('E'), Attribute('a'), 'abc', True,
                             Float(5.0), 5.0, 5, datetime.datetime(2019, 1, 1),  
                             Bytes([0x5A,0x26]), ByteString([0x5A,0x26]), Chars('x\\ "'), Index(3)])
        
    def test_flatten_to_tuples(self):
        s = flatten_to_tuples({'a':{'b':1}}, flattened_lists=True, compact_tips=False)
        self.assertEqual(s, {('a','b',1): None})
        s = flatten_to_tuples({'a':{'b':1}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a','b',1): None})
        
        s = flatten_to_tuples({'a':{'b':None}}, flattened_lists=True, compact_tips=False)
        self.assertEqual(s, {('a','b'): None})
        s = flatten_to_tuples({'a':{'b':None}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a','b'): None})
        
        E = EntityClass('E')
        s = flatten_to_tuples({'a':{E: {'b':None}}}, flattened_lists=True, compact_tips=None)
        self.assertEqual(s, {('a',): {E: {('b',): None}}})
        s = flatten_to_tuples({'a':{E: {'b':{'c':None}}}}, flattened_lists=True, compact_tips=None)
        self.assertEqual(s, {('a',): {E: {('b','c'): None}}})
        s = flatten_to_tuples({'a':{'b':{E: {'c':{'d':None}}}}}, flattened_lists=True, compact_tips=None)
        self.assertEqual(s, {('a','b'): {E: {('c','d'): None}}})
        s = flatten_to_tuples({'a':{'b':{E: {'c':{'d':None,'e':None}}}}}, flattened_lists=True, compact_tips=None)
        self.assertEqual(s, {('a','b'): {E: {('c','d'): None, ('c','e'): None}}})
        s = flatten_to_tuples({'a':{'b':{E: {'c':None,'d':None}}}}, flattened_lists=True, compact_tips=None)
        self.assertEqual(s, {('a','b'): {E: {('c',): None, ('d',): None}}})

        # compacting tips is probably not that interesting
        s = flatten_to_tuples({'a':{E: {'b':None}}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a',): {E: 'b'}})
        s = flatten_to_tuples({'a':{E: {'b':{'c':None}}}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a',): {E: {('b','c'): None}}})
        s = flatten_to_tuples({'a':{'b':{E: {'c':{'d':None}}}}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a','b'): {E: {('c','d'):None}}})
        # you would not flatten twice, but this is what happens if you do
        s = flatten_to_tuples({'a':{'b':{E: {('c','d'):None,('c','e'):None}}}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a','b'): {E: {(('c','d'),):None,(('c','e'),):None}}})
        s = flatten_to_tuples({'a':{'b':{E: {'c':None,'d':None}}}}, flattened_lists=True, compact_tips=True)
        self.assertEqual(s, {('a','b'): {E: {('c',): None, ('d',): None}}})

    def test_flatten_lists_to_indexes(self):
        s = flatten_lists_to_indexes(['a','b'])
        self.assertEqual(s, {Index(0): 'a',Index(1): 'b'})
        s = flatten_lists_to_indexes(['a',['b','c']])
        self.assertEqual(s, {Index(0): 'a',Index(1): { Index(0):'b',Index(1): 'c'}})
    
    def test_unflatten_lists_from_indexes(self):
        s = unflatten_lists_from_indexes({Index(0): 'a',Index(1): 'b'})
        self.assertEqual(s, ['a','b'])

    def test_to_json_extended(self):
        s = to_json_extended({'a':1,'b':2,'_c':3,EntityClass('EC'):True,Attribute('att'):False,Bytes([0x5A,0x26]):Bytes([0x5A,0x26]),ByteString([0x5A,0x26]):ByteString([0x5A,0x26]),Index(3):Index(3),Chars('ccc'):Chars('ccc')})
        s = re.sub(r'\s', '', s)
        self.assertEqual(s, '{"a":1,"b":2,"_c":3,EC:true,att:false,Bytes(5A_26):Bytes(5A_26),ByteString(5A_26):ByteString(5A_26),[3]:[3],Chars("ccc"):Chars("ccc")}')   

    def test_unflatten_from_tuples(self):
        t1 = ('a', 0)
        t2 = ('b', 1)

        s1 = unflatten_from_tuples(t1)
        s2 = unflatten_from_tuples(t2)

        self.assertEqual(s1, {'a': {0: None}})
        self.assertEqual(s2, {'b': {1: None}})

        
    def test_underscore_quote(self):
        s = underscore_quote({True:False,
                             False:False,
                             'a':1,
                             'b':+2,
                             '_c':-3e999,
                             EntityClass('EC'):EntityClass('EC'),
                             Attribute('att'):Attribute('att'),
                             5:None,
                             6.0:6.0,
                             datetime.datetime(2019, 1, 1):datetime.datetime(2019, 1, 1),
                             Float(7.0):Float(7.0),
                             Chars('ccc'):Chars('ccc'),
                             Bytes([0x5A,0x26]):Bytes([0x5A,0x26]),
                             ByteString([0x5A,0x26]):ByteString([0x5A,0x26]),
                             Index(3):Index(3),
                             'list': ['a','b',3,Float(6)],
                             'dict': {'a':1,'b':2},
                             'tuple': ('a','b')})
        s2 = {"_true":False,
            "_false":False,
            "a":1,
            "b":2,
            "__c":-3e999,
            "_EC":"_EC",
            "_att":"_att",
            "_5":None,
            "_6.0":6.0,
            "_2019-01-01T00:00:00":"_2019-01-01T00:00:00",
            "_7.0f":"_7.0f",
            "_Chars(\"ccc\")":"_Chars(\"ccc\")",
            "_Bytes(5A_26)":"_Bytes(5A_26)",
            "_ByteString(5A_26)":"_ByteString(5A_26)",
            "_[3]":"_[3]",
            "list":["a","b",3,'_6.0f'],
            "dict":{"a":1,"b":2},
            "tuple":("a","b")}
        self.assertEqual(s, s2)

    def test_underscore_unquote(self):
        s = {True:False,
                False:False,
                'a':1,
                'b':+2,
                '_c':-3e999,
                EntityClass('EC'):EntityClass('EC'),
                Attribute('att'):Attribute('att'),
                5:None,
                6.0:6.0,
                datetime.datetime(2019, 1, 1):datetime.datetime(2019, 1, 1),
                Float(7.0):Float(7.0),
                Chars('ccc'):Chars('ccc'),
                Bytes([0x5A,0x26]):Bytes([0x5A,0x26]),
                ByteString([0x5A,0x26]):ByteString([0x5A,0x26]),
                Index(3):Index(3),
                'list': ['a','b',3,Float(6)],
                'dict': {'a':1,'b':2},
                'tuple': ('a','b')}
        s2 = underscore_unquote({"_true":False,
            "_false":False,
            "a":1,
            "b":2,
            "__c":-3e999,
            "_EC":"_EC",
            "_att":"_att",
            "_5":None,
            "_6.0":6.0,
            "_2019-01-01T00:00:00":"_2019-01-01T00:00:00",
            "_7.0f":"_7.0f",
            "_Chars(\"ccc\")":"_Chars(\"ccc\")",
            "_Bytes(5A_26)":"_Bytes(5A_26)",
            "_ByteString(5A_26)":"_ByteString(5A_26)",
            "_[3]":"_[3]",
            "list":["a","b",3,'_6.0f'],
            "dict":{"a":1,"b":2},
            "tuple":("a","b")})
        self.assertEqual(s, s2)

def test_compact_tree_tips():
    s = compact_tree_tips({'a':{'b':1}})
    self.assertEqual(s, {'a':{'b':1}})
    s= compact_tree_tips({'a':{'b':None}})
    self.assertEqual(s, {'a':'b'})
    s = compact_tree_tips({'a':{'b':{'c':None}}})
    self.assertEqual(s, {'a':{'b':'c'}})
    s = compact_tree_tips({'a':{'b':{'c':{'d':None}}}})
    self.assertEqual(s, {'a':{'b':{'c':'d'}}})
    s = compact_tree_tips({'a':{'b':{'c':{'d':None,'e':None}}}})
    self.assertEqual(s, {'a':{'b':{'c':{'d':None,'e':None}}}})
    s = compact_tree_tips({'a':{'b':{'c':None,'d':1}}})
    self.assertEqual(s, {'a':{'b':{'c': None,'d':1}}})
    
def test_uncompact_tree_tips():
    s = compact_tree_tips({'a':{'b':1}})
    self.assertEqual(s, {'a':{'b':{1:None}}})
    s= compact_tree_tips({'a':{'b':None}})
    self.assertEqual(s, {'a':{'b':None}})
    s = compact_tree_tips({'a':{'b':{'c':None}}})
    self.assertEqual(s, {'a':{'b':{'c':None}}})
    s = compact_tree_tips({'a':{'b':{'c':{'d':1}}}})
    self.assertEqual(s, {'a':{'b':{'c':{'d':{1:None}}}}})
    s = compact_tree_tips({'a':{'b':{'c':{'d':None,'e':1}}}})
    self.assertEqual(s, {'a':{'b':{'c':{'d':None,'e':{1:None}}}}})
    s = compact_tree_tips({'a':{'b':{'c':None,'d':None}}})
    self.assertEqual(s, {'a':{'b':{'c': None,'d':None}}})
                     
if __name__ == '__main__':
    unittest.main()
    
    
