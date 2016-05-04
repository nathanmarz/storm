#!/usr/bin/python
# -*- coding: utf-8 -*-
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

def encode(obj, encoding='UTF-8'):
    """
    Check if the object supports encode() method, and if so, encodes it.
    Encoding defaults to UTF-8.
    For example objects of type 'int' do not support encode
    """
    return obj.encode(encoding) if 'encode' in dir(obj) else obj

class Formatter:
    def __init__(self, fields_tuple=(), row_tuple=(), min_width_tuple=None):
        # Format to pass as first argument to the print function, e.g. '%s%s%s'
        self.format = ""
        # data_format will be of the form ['{!s:43}'],'{!s:39}','{!s:11}','{!s:25}']
        # the widths are determined from the data in order to print output with nice format
        # Each entry of the data_format list will be used by the advanced string formatter:
        # "{!s:43}".format("Text")
        # Advanced string formatter as detailed in here: https://www.python.org/dev/peps/pep-3101/
        self.data_format = []
        Formatter._assert(fields_tuple, row_tuple, min_width_tuple)
        self._build_format_tuples(fields_tuple, row_tuple, min_width_tuple)

    @staticmethod
    def _assert(o1, o2, o3):
        if len(o1) != len(o2) and (o3 is not None and len(o2) != len(o3)):
            raise RuntimeError("Object collections must have the same length. "
                               "len(o1)={0}, len(o2)={1}, len(o3)={2}"
                               .format(len(o1), len(o2), -1 if o3 is None else len(o3)))

    # determines the widths from the data in order to print output with nice format
    @staticmethod
    def _find_sizes(fields_tuple, row_tuple, min_width_tuple):
        sizes = []
        padding = 3
        for i in range(0, len(row_tuple)):
            max_len = max(len(encode(fields_tuple[i])), len(str(encode(row_tuple[i]))))
            if min_width_tuple is not None:
                max_len = max(max_len, min_width_tuple[i])
            sizes += [max_len + padding]
        return sizes

    def _build_format_tuples(self, fields_tuple, row_tuple, min_width_tuple):
        sizes = Formatter._find_sizes(fields_tuple, row_tuple, min_width_tuple)

        for i in range(0, len(row_tuple)):
            self.format += "%s"
            self.data_format += ["{!s:" + str(sizes[i]) + "}"]

    # Returns a tuple where each entry has a string that is the result of
    # statements with the pattern "{!s:43}".format("Text")
    def row_str_format(self, row_tuple):
        format_with_values = [str(self.data_format[0].format(encode(row_tuple[0])))]
        for i in range(1, len(row_tuple)):
            format_with_values += [str(self.data_format[i].format(encode(row_tuple[i])))]
        return tuple(format_with_values)
