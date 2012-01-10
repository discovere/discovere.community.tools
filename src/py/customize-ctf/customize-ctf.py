################################################################################
################################################################################
##
##   Copyright 2012 discover-e Legal, LLC
##
##   Licensed under the Apache License, Version 2.0 (the "License");
##   you may not use this file except in compliance with the License.
##   You may obtain a copy of the License at
##
##       http://www.apache.org/licenses/LICENSE-2.0
##
##   Unless required by applicable law or agreed to in writing, software
##   distributed under the License is distributed on an "AS IS" BASIS,
##   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##   See the License for the specific language governing permissions and
##   limitations under the License.
##
################################################################################
################################################################################

import re
import string
import exceptions
import binascii 
import os
import sys

####################
## CSV support
####################
import csv
import cStringIO
import codecs

csv.register_dialect('ctf', quoting=csv.QUOTE_ALL)
csv.field_size_limit(sys.maxint)
should_pause = True     # hack hack hack, global varEEUBLULZ!

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8-sig') for cell in row]

def open_ctf(ctf_path):
    ctf_file = open(ctf_path) #, 'rb') #codecs.open(ctf_path, 'rb', encoding='utf-8-sig') 
    
    reader = unicode_csv_reader(ctf_file, dialect='ctf')
    header = reader.next()
    return header, reader


####################
## DSL Functions
####################

def encode(encoding, fields):
    #encoding: An encoding
    for field in fields:
        if encoding == 'base64':
            field.value = binascii.b2a_base64(field.value)
        elif encoding == 'hex':
            field.value = binascii.b2a_hex(field.value).upper()
        elif encoding == 'hqx':
            field.value = binascii.b2a_hqx(field.value)
        elif encoding == 'qp':
            field.value = binascii.b2a_qp(field.value)
        elif encoding == 'uu':
            field.value = binascii.b2a_uu(field.value)
        yield field

def decode(encoding, fields):
    #encoding: An encoding
    for field in fields:
        if encoding == 'base64':
            field.value = binascii.a2b_base64(field.value)
        elif encoding == 'hex':
            field.value = binascii.a2b_hex(field.value)
        elif encoding == 'hqx':
            field.value = binascii.a2b_hqx(field.value)
        elif encoding == 'qp':
            field.value = binascii.a2b_qp(field.value)
        elif encoding == 'uu':
            field.value = binascii.a2b_uu(field.value)
        yield field
        
def divide(divisor, precision, fields):
    #fields A list of fields
    #divisor: Any numeric expression
    #precision_format: A string indicating the level of precison to output
    try:
        divisor = int(divisor)
    except:
        raise Exception("divide: Parameter 'divisor' must be an integer. Value provided was {0}".format(divisor))
    try:
        precision = int(precision)
    except:
        raise Exception("divide: Parameter 'precision' must be an integer. Value provided was {0}".format(precision))

    for field in fields:
        try:
            numvalue = float(field.value) if '.' in field.value else int(field.value)
            numvalue = numvalue / float(divisor)            
        except exceptions.ValueError:
            numvalue = 0
        
        field.value = ("%."+ str(precision)+ "f") % numvalue
        yield field

def exists(fields):
    #Returns a "True"/"False" value for any input field name, if the field is populated
    for field in fields:
        if field.value <> "": 
            field.value = "True"
        else:
            field.value = "False"
    
        yield field

def populated(fields):
    #Returns only the fields that are populated
    for field in fields:
        if field.value.strip() <> "": 
            yield field

def replace(find, replace, fields):
    # Replaces a string in a string
    
    # replace is either a list of fields, an individual field, or a string
    
    # if it's an iterable, it's a list of fields
    if hasattr(replace, '__iter__'):
        selector = [x for x in replace]
        if len(selector) > 0:
            replace = final(selector)
        else:
            replace = ""
    
    # if it has a value property it's a Field object
    if hasattr(replace, 'value'):
        replace = replace.value
        
    for field in fields:
        field.value = string.replace(field.value, find, replace)
        yield field

def format(format, fields):
    #format: A format string where {name} = field name, {value} = field value
    #    ex: "{name}: {value}"  # other props format
    for field in fields:
        newvalue = string.replace(format, "{name}", field.name)
        newvalue = string.replace(newvalue, "{value}", field.value)
        field.value = newvalue
        yield field

def join(delimiter, fields):
    #Merges a list of values with a delimiter
    
    #<field-list>: A list expression of fields to merge
    #delimiter: A delimiter used to separate records
    if not hasattr(fields, '__iter__'):
        return fields
    
    fields = [x for x in fields]
    names = [x.name for x in fields]
    values = [x.value for x in fields]
    
    
    name = string.join(names, "_")
    value = string.join(values, delimiter)
    return [Field(name, value)]

def first(fields):
    # Returns the first field in the list
    if not hasattr(fields, '__iter__'):        
        if fields.value <> "": 
            yield fields
    else:
        for field in fields:
            if field.value <> "":
                yield field
                break

def regex_match(pattern, fields):
    # $"<field-regex>":<field-list>
    #    Finds fields with names that match the regex
    for field in fields:        
        if re.search(pattern, field.name):
            yield field

def similar_match(pattern, fields):
    # ~"<field-name>":<field-list>
    #     Finds field with names similar to the provided field using custom logic
    for field in fields:
        if field.name.lower() == pattern.lower():
            yield field
        
        if re.search(pattern.lower() + "_\d+$", field.name.lower()):
            yield field
        
def exact_match(pattern, fields):
    # @"<field-name>":<field>
    #    Selects a field by exact name. Quotes are optional.
    
    for field in fields:
        if field.name == pattern: 
            yield field

def final(fields):
    # Returns the final form of the field value
    if not hasattr(fields, '__iter__'):
        return fields

    results = [x for x in fields]
    if len(results) > 0:
        result = results[0]
    else:
        result = Field("","")
        
    result.value = result.value
    
    return result
    
#####################
## Parsing
#####################

def skip(val, exp):
    if exp.startswith(val):
        return exp[len(val):]
    else:
        return exp

def read_until(val, exp):
    if None == val:
        result = exp
        tail = None
    else:
        exp = exp.strip()
        i = 0
        in_quotes = False
        parens = 0
        result = exp
        tail = None
        while i < len(exp):
            window = exp[i:]
            if exp[i] == '"':
                if not in_quotes:
                    in_quotes = True
                else:
                    in_quotes = False

            if val == ")" and exp[i] == '(':                
                parens += 1

            if val == ")" and exp[i] == ')' and parens > 0:
                parens -= 1
            else:
                if not in_quotes and window.startswith(val):
                    result = exp[:i]
                    tail = skip(val, exp[i:]).strip()
                    break
                    
            i += 1
            
    result = result.strip()
    
    result = unwrap_quotes(result)
    
    return result, tail
    
def unwrap_quotes(val):
    if len(val) > 0 and val.startswith('"') and val.endswith('"'):
        val = val[1:][:-1]
    return val
        
def eval(expression, input, terminal=None):

    if expression.startswith("("):
        expression = skip("(", expression)
        field_exps, expression = read_until(")", expression)
        
        input = [x for x in input]
        node = []        
        while field_exps != None:
            if field_exps.startswith("@"):
                field_exps = skip("@", field_exps)
                literal_name, field_exps = read_until(",", field_exps)
                field = final(exact_match(literal_name, input))
            else:
                literal_name, field_exps = read_until(",", field_exps)
                field = Field("<constant-value>", literal_name)
                
            if field.value <> "":
                node.append(field)
        
    elif expression.startswith("@"):
        literal_name, expression = read_until(terminal, skip("@", expression))
        node = exact_match(literal_name, input)
        
    elif expression.startswith("~"):
        similar_name, expression = read_until(terminal, skip("~", expression))
        node = similar_match(similar_name, input)
        
    elif expression.startswith("$"):
        regex_name, expression = read_until(terminal, skip("$", expression))
        node = regex_match(regex_name, input)
        
    elif expression.startswith("first("):
        field_exp, expression = read_until(")", skip("first(", expression))
        node = first(eval(field_exp, input))
        
    elif expression.startswith("join("):
        delim, expression = read_until(",", skip("join(", expression))
        field_exp, expression = read_until(")", expression)
        node = join(delim, eval(field_exp, input))
        
    elif expression.startswith("format("):
        format_str, expression = read_until(",", skip("format(", expression))
        field_exp, expression = read_until(")", expression)
        node = format(format_str, eval(field_exp, input))
        
    elif expression.startswith("encode("):
        encoding, expression = read_until(",", skip("encode(", expression))
        field_exp, expression = read_until(")", expression)
        node = encode(encoding, eval(field_exp, input))   
        
    elif expression.startswith("decode("):
        encoding, expression = read_until(",", skip("decode(", expression))
        field_exp, expression = read_until(")", expression)
        node = decode(encoding, eval(field_exp, input))   

    elif expression.startswith("replace("):
        expression = skip("replace(", expression)
        p1, expression = read_until(",", expression)
        p2, expression = read_until(",", expression)
        field_exp, expression = read_until(")", expression)
        
        if p2.startswith("@"):
            p2 = eval(p2, input)
            
        node = replace(p1, p2, eval(field_exp, input))
        
    elif expression.startswith("exists("):
        expression = skip("exists(", expression)
        field_exp, expression = read_until(")", expression)
        node = exists(eval(field_exp, input))
        
    elif expression.startswith("populated("):
        expression = skip("populated(", expression)
        field_exp, expression = read_until(")", expression)
        node = populated(eval(field_exp, input))
        
    elif expression.startswith("divide("):
        expression = skip("divide(", expression)
        p1, expression = read_until(",", expression)
        p2, expression = read_until(",", expression)
        field_exp, expression = read_until(")", expression)
        node = divide(p1, p2, eval(field_exp, input))        
    else:
        if expression != None and expression <> "":
            return [Field("<constant-value>", expression)]
            #raise Exception("Unknown expression: {0}".format(expression))
        else:
            return input
        
    if None == expression: 
        return node
    else:
        return eval(expression, node)
    
def convert(expression, fields, scalar=True):
    results = eval(expression, fields)
    if scalar:    
        return final(results)
    else:
        return results

def read_ini(file):
    
    results = { 'default': {}, 'sections': [] }
    
    current_section = None
    while 1:
        line = file.readline()
        if not line:
            break
        
        # ignore comments
        if line.startswith('#') or line.startswith(';'):
            continue
        
        # read section head
        if line.startswith('['):
            section_name, tail = read_until(']', skip('[', line))

            if current_section != None:
                if current_section['name'].lower() == 'default':
                    #print "read defaults"
                    results['default'] = current_section['values']
                elif current_section['name'].lower() == 'group':
                    #print "read groups"
                    results['group'] = current_section['values']
                else:
                    #print "read section", current_section['name']
                    results['sections'].append(current_section)
            
            current_section = { 'name': section_name, 'values': [] }
            continue
            
        value_name, value_expression = read_until('=', line)
        if value_name <> "":
            current_section['values'].append([value_name, value_expression])
    
    results['sections'].append(current_section)
    return results
    
###############################
## business logic
###############################

class Field():
    def __init__(self, name, value):
        self.name = name
        self.value = value

def get_row_group(group_field_value, settings):
        #determine group for row
        row_group = 'default'        
        for group in settings['group']:            
            for group_rule in group[1]:
                #print group[0], group_rule, group_field_value
                if re.search(group_rule, group_field_value):
                    return group[0]

def updated_rows(header, reader, settings):
    group_field_name = settings['default']['group_field']
    
    group_field_index = None
    if group_field_name in header:
        group_field_index = header.index(settings['default']['group_field'])
    
    # need to do section look up by name, but also maintain order for groups... so this.
    sections = {}
    for section in settings['sections']:
        section_values = {}
        for section_value in section['values']:
            section_values[section_value[0]] = section_value[1]
            
        sections[section['name']] = section_values
        
    for row in reader:
        fields = [Field(header[i], row[i]) for i in range(0, len(header))]
        
        row_group = 'default'
        if group_field_index != None: 
            row_group = get_row_group(row[group_field_index], settings)
        #print "group =>", row_group
        
        newrow = []
        for section in settings['sections']:
            section_values = sections[section['name']]
            
            group_to_use = 'default'
            if row_group in section_values:
                group_to_use = row_group
                
            if group_to_use in section_values:
                rule = section_values[group_to_use]
                output_value = convert(rule, fields).value
                #print rule, "=>", output_value.encode('utf-8')
                newrow.append(output_value)
                
        yield newrow

def handle_settings():

    real_cwd = os.path.dirname(sys.argv[0])
    settings_file = os.path.join(real_cwd, "default.ini")
    should_pause = True
    
    print
    print "settings:"
    print string.ljust("  file", 11), ":", settings_file
    
    if len(sys.argv) > 2:
        if sys.argv[2] == '-nopause':
            should_pause = False
        else:
            settings_file = sys.argv[2]

    if len(sys.argv) > 3:
        if sys.argv[3] == '-nopause':
            should_pause = False
        else:
            settings_file = sys.argv[3]
            
    settings_file = open(settings_file)
    settings = read_ini(settings_file)
    settings_file.close()
    
    # turn defaults into dictionary, because order isn't import for this section
    new_default = {}
    for value in settings['default']:
        new_default[value[0]] = value[1]
    
    settings['default'] = new_default

    # default settings
    default_group_field_name = "MimeType"
    if not 'group_field' in settings['default']:
        settings['default']['group_field'] = default_group_field_name
    
    default_partition = sys.maxint
    if 'partition' in settings['default']:
        settings['default']['partition'] = int(settings['default']['partition'])
    else:
        settings['default']['partition'] = default_partition
        
    print
    print "options:"
    print "  group by  :", settings['default']['group_field']
    print "  partition :", settings['default']['partition']
    print

    # group settings
    print "groups:"
    if not 'group' in settings:
        settings['group']=[]
    for i in range(0, len(settings['group'])):
        group = settings['group'][i]
        
        # parse out lists of mime type expressions
        group[1] = [x.value for x in convert(group[1], [], False)]
        print string.ljust("  " + group[0], 11),  ":", string.join(group[1], ',')
    print 

    # export settings
    print "export fields:"
    for section in settings['sections']:
        print string.ljust("  field", 11),  ":", section['name']
    print
    
    return settings

def setup_writer(partition, ctf_path, headers): 
    _, extension = os.path.splitext(ctf_path)
    
    suffix_format = '.new{0}'
    if partition > 0:
        suffix_format = '.new.{1}{0}'
        
    new_ctf_path = ctf_path.replace(extension, suffix_format.format(extension, partition))
        
    print "  file      : {0}".format(os.path.basename(new_ctf_path))
    
    new_ctf_file = open(new_ctf_path, 'wb')
    new_ctf_file.write(u'\ufeff'.encode('utf-8'))
    writer = UnicodeWriter(new_ctf_file, dialect='ctf')
    
    writer.writerow(headers)
    return new_ctf_file, writer

def usage():
    print
    print "usage: " + sys.argv[0] + " <ctf-file> [<settings-file>|-nopause]"
    print 
    print "  <ctf-file>      - (required) The CTF/CSV to process"
    print "  <settings-file> - (optional) A settings file. If not specified default.ini will be used."
    print "  -nopause        - (optional) Do not pause for user input after completion."
    print

def validate_parameters():
    
    result = True
    errors = []
    
    if len(sys.argv) < 2:
        usage()
        result = False
    
    if len(sys.argv) > 1:
        if not os.path.exists(sys.argv[1]):            
            errors.append("  Error : <ctf-file> parameter specified a file that does not exist.")
            errors.append("  File  : " + sys.argv[1])
            errors.append("")
            result = False

    if len(sys.argv) > 2:
        if sys.argv[2] != '-nopause' and not os.path.exists(sys.argv[2]):            
            errors.append("  Error : [settings-file] parameter specified a file that does not exist.")
            errors.append("  File  : " + sys.argv[2])
            errors.append("")
            result = False

    if len(sys.argv) > 3:
        if sys.argv[3] != '-nopause' and not os.path.exists(sys.argv[3]):
            errors.append("  Error : [settings-file] parameter specified a file that does not exist.")
            errors.append("  File  : " + sys.argv[3])
            errors.append("")
            result = False
            
    if not result:
        usage()
        for error in errors:
            print >> sys.stderr, error
            
    return result

def main():
    
    if not validate_parameters():
        return
    
    # handle settings 
    settings = handle_settings()

    # handle input/output files    
    # input CTF
    
    ctf_path = sys.argv[1]

    print "input:" 
    print "  file      : {0}".format(ctf_path)
    print

    header, reader = open_ctf(ctf_path)
   
    
    # headers
    # fix first header..
    if len(header) > 0:
        header[0] = unwrap_quotes(header[0])

    output_headers = [x['name'] for x in settings['sections']]
    
    # output CTF
    print "output:"
    new_ctf_file, writer = setup_writer(0, ctf_path, output_headers)

    rowcount = 0
    partition = 0
    for row in updated_rows(header, reader, settings):
        rowcount += 1
        if rowcount > settings['default']['partition']:
            partition += 1
            new_ctf_file.close()
            new_ctf_file, writer = setup_writer(partition, ctf_path, output_headers)
            rowcount = 0
        writer.writerow(row)
    
    try:
        new_ctf_file.close()
    except:
        pass
    
if __name__ == '__main__':
    
    try:
        main()
    except:
        import traceback
        traceback.print_exc()
    
    print
    if should_pause:
        os.system("pause")
    