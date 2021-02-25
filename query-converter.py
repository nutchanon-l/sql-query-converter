import json
import getopt
import sys
import re
import traceback
import itertools

class ConverterOption:
    def __init__(self, filename, target_service, query_type, output_file, verbose):
        self.filename = filename
        self.target_service = target_service
        self.query_type = query_type
        self.output_file = output_file
        self.verbose = verbose

def get_opt() -> ConverterOption:
    # get options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvf:s:t:o:", ["help", "file=", "service=", "type=", "output=", "verbose"])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        
    # declare variables
    filename = None
    target_service = None
    query_target_style = None
    output_file = None
    verbose = False
    
    # check options
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-f", "--file"):
            filename = a
        elif o in ("-s", "--service"):
            target_service = a
        elif o in ("-t", "--type"):
            query_target_style = a
        elif o in ("-o", "--output"):
            output_file = a
        elif o in ("-v", "--verbose"):
            verbose = True
        else:
            assert False, "unhandled option"
    
    return ConverterOption(filename, target_service, query_target_style, output_file, verbose)

def usage():
    print("How to use this script...")
    print("$ python query-converter.py [options]")
    print("-f, --file : input filename")
    print("-s, --service : Amazon services [redshift or athena]")
    print("-t, --type : convert type")
    print("    athena type 1: convert LIKE to regexp")
    print("    athena type 2: convert LIKE to regexp and combine AND with permutations")
    print("    redshift type 1: convert OR to text ~ '(.*word1.*)|(.*word2.*)' and leave AND as-is")
    print("    redshift type 2: convert OR to text ~ '(.*word1.*)|(.*word2.*)' and combine AND with permutation")
    print("    redshift type 3: convert OR to text ~ 'word1|word2' and leave AND as-is")
    print("    redshift type 4: convert OR to text ~ 'word1|word2' and combine AND with permutation")
    print("    redshift type 5: convert LIKE with regexp_multi_match and bitstring functions")
    print("-o, --output : output file")
    print("-h, --help : help")
    print("-v, --verbose : verbose converted statements")
    
def read_sql_statements(filename: str) -> list:
    # read file
    with open(filename, "r") as f:
        all_stmts = f.read()
    f.close()
    
    # split string with ';' for each sql statements
    stmts = list(filter(None, all_stmts.split(';')))
    formated_stmts = []
    
    # format statement by replacing newline and tab
    for stmt in stmts:
        formated_stmt = stmt.replace("\n", " ").replace("\t", "").strip()
        formated_stmts.append(formated_stmt)
    return formated_stmts
    
def write_output_statement(filename: str, stmts: str, verbose: bool):
    if type(stmts) is list:
        stmts = ''.join(stmts)
    
    if verbose is True and filename is None:
        print(stmts)
    elif verbose is True and filename is not None:
        print(stmts)
        with open(filename, "w+") as f:
            f.write(stmts)
        f.close()
    else:
        with open(filename, "w+") as f:
            f.write(stmts)
        f.close()
    
def convert_statements(stmts: list, target_service: str, query_type: str) -> list:
    
    try:
        new_stmts = []
        for stmt in stmts:
            # split WHERE clause
            splited_stmt = regex_split_like(stmt, 'where')
            
            # check unexpected statement
            if len(splited_stmt) != 2:
                raise ValueError("Could not split WHERE clause of '{}'".format(stmt))
            
            front_stmt = splited_stmt[0]
            back_stmt = splited_stmt[1]
        
            # split OR clause
            splited_or = regex_split_like(back_stmt, 'or')
            splited_or_stmt = [x.strip().replace('(','').replace(')','') for x in splited_or]
            
            # print number of total
            print('Number of keywords: {}'.format(len(splited_or_stmt)))
        
            if target_service == "athena":
                # split AND clause
                if query_type == "1":
                    new_conn = convert_athena_type_1(splited_or_stmt)
                elif query_type == "2":
                    new_conn = convert_athena_type_2(splited_or_stmt)
    
                # combine with OR
                new_back_stmt = ' OR '.join(new_conn)
                
                # combine to completed statement
                new_stmt = "{} WHERE {} \n;\n".format(front_stmt, new_back_stmt)
                new_stmts.append(new_stmt)
                
            elif target_service == "redshift":
                if query_type == "5":
                    new_stmt = convert_redshift_type_5(front_stmt, splited_or_stmt)
                    new_stmts.append(new_stmt)
                else:
                    if query_type == "1":
                        print('Work-in-progress !!')
                        new_conn = []
                    elif query_type == "2":
                        print('Work-in-progress !!')
                        new_conn = []
                    elif query_type == "3":
                        new_conn = convert_redshift_type_3(splited_or_stmt)
                    elif query_type == "4":
                        new_conn = convert_redshift_type_4(splited_or_stmt)
                    elif query_type == "5":
                        new_conn = convert_redshift_type_5(front_stmt, splited_or_stmt)
                
                    # combine with OR
                    new_back_stmt = '{}'.format(new_conn[0])
                
                    # combine to completed statement
                    new_stmt = "{} WHERE {} \n;\n".format(front_stmt, new_back_stmt)
                    new_stmts.append(new_stmt)
                
    except ValueError as e:
        print(e)
        sys.exit(2)
    except Exception as e:
        print("Unknown error exception")
        traceback.print_exc()
        sys.exit(2)
    
    return new_stmts

def regex_split_like(text: str, split_word: str) -> list:
    upper_word = split_word.upper()
    lower_word = split_word.lower()
    # regex_for_parentheses = "(?![^(]*\))"
    regex_for_like_stmt = "(?![^'%]*%')"
    kv = re.split('{}{}|{}{}'.format(upper_word, regex_for_like_stmt, lower_word, regex_for_like_stmt),text)
    trim_kv = [x.strip() for x in kv]
    return trim_kv

def kv_like_split(conn):
    kv = regex_split_like(conn, 'like')
    k = kv[0].strip()
    v = kv[1].strip().replace('\'','').replace('%','')
    return k, v

def convert_athena_type_1(splited_or_stmt: list) -> list:
    new_conn = []
    for conn_and in splited_or_stmt:
        kv_and = regex_split_like(conn_and, 'and')
        if len(kv_and) > 1:
            new_ands = []
            for x in kv_and:
                k, v = kv_like_split(x)
                new_ands.append('(regexp_like({},\'{}\'))'.format(k,v))
            and_stmt = ' AND '.join(new_ands)
            new_conn.append('({})'.format(and_stmt))
        else:
            for x in kv_and:
                k, v = kv_like_split(x)
                new_conn.append('(regexp_like({},\'{}\'))'.format(k,v))
    return new_conn

def convert_athena_type_2(splited_or_stmt: list) -> list:
    new_conn = []
    or_keyword_count = 0
    and_keyword_count = 0
    for conn_and in splited_or_stmt:
        kv_and = regex_split_like(conn_and, 'and')
        if len(kv_and) > 1:
            new_ks = []
            new_vs = []
            for x in kv_and:
                and_keyword_count = and_keyword_count + 1
                k, v = kv_like_split(x)
                new_ks.append(k)
                new_vs.append(v)
            
            if len(list(dict.fromkeys(new_ks))) > 1:
                raise ValueError("Unexpected 'text' in AND fields")
                
            new_and_permutation = list(itertools.permutations(new_vs,len(new_vs)))
            for and_permu in new_and_permutation:
                regex_and = '.*'.join(list(and_permu))
                new_conn.append('(regexp_like({},\'{}\'))'.format(new_ks[0],regex_and))
        else:
            or_keyword_count = or_keyword_count + 1
            for x in kv_and:
                k, v = kv_like_split(x)
                new_conn.append('(regexp_like({},\'{}\'))'.format(k,v))
    
    print('Number of OR keywords: {}'.format(or_keyword_count))
    print('Number of AND keywords: {}'.format(and_keyword_count))
    
    return new_conn

def convert_redshift_type_3(splited_or_stmt: list) -> list:
    new_keys = []
    new_values = []
    new_and_values = []
    regex_for_like_stmt = "(?![^'%]*%')"
    for conn_and in splited_or_stmt:
        if re.search('and{}|AND{}'.format(regex_for_like_stmt, regex_for_like_stmt), conn_and):
            new_and_values.append('({})'.format(conn_and))
        else:
            k, v = kv_like_split(conn_and)
            new_keys.append(k)
            new_values.append(v)
    
    # check if key has more than 1
    new_key = list(dict.fromkeys(new_keys))
    if len(new_key) > 1:
        raise ValueError("Unexpected 'text' in AND fields")
    
    # combine
    new_or_stmt = '{} ~ \'{}\''.format(new_key[0], '|'.join(new_values))
    new_conn = [new_or_stmt + " OR " + ' OR '.join(new_and_values)]
    
    return new_conn

def convert_redshift_type_4(splited_or_stmt: list) -> list:
    new_keys = []
    new_values = []
    for conn_and in splited_or_stmt:
        kv_and = regex_split_like(conn_and, 'and')
        if len(kv_and) > 1:
            new_vs = []
            for x in kv_and:
                k, v = kv_like_split(x)
                new_keys.append(k)
                new_vs.append(v)
                
            new_and_permutation = list(itertools.permutations(new_vs,len(new_vs)))
            for and_permu in new_and_permutation:
                regex_and = '.*'.join(list(and_permu))
                new_values.append(regex_and)
        else:
            for x in kv_and:
                k, v = kv_like_split(x)
                new_keys.append(k)
                new_values.append(v)
    
    # check if key has more than 1
    new_key = list(dict.fromkeys(new_keys))
    if len(new_key) > 1:
        raise ValueError("Unexpected 'text' in AND fields")
    
    # combine
    new_conn = ['{} ~ \'{}\''.format(new_key[0], '|'.join(new_values))]
    return new_conn

def convert_redshift_type_5(front_stmt: str, splited_or_stmt: list) -> list:
    # new front statement
    front_list = regex_split_like(front_stmt, 'from')
    table_name = front_list[1]
    colume_name = 'id'
    temp_table_name = 'temp_table'
    
    # new back statement
    new_conn = []
    new_keys = []
    new_values = []
    count_word = 1
    for conn_and in splited_or_stmt:
        kv_and = regex_split_like(conn_and, 'and')
        if len(kv_and) > 1:
            new_ands = []
            for x in kv_and:
                k, v = kv_like_split(x)
                new_keys.append(k)
                new_values.append(v)
                new_ands.append('(bitstring_read(bitstr, {}))'.format(count_word))
                count_word = count_word + 1
            and_stmt = ' AND '.join(new_ands)
            new_conn.append('({})'.format(and_stmt))
        else:
            for x in kv_and:
                k, v = kv_like_split(x)
                new_keys.append(k)
                new_values.append(v)
                new_conn.append('(bitstring_read(bitstr, {}))'.format(count_word))
                count_word = count_word + 1
    
    # check if key has more than 1
    new_key = list(dict.fromkeys(new_keys))
    if len(new_key) > 1:
        raise ValueError("Unexpected 'text' in AND fields")

    # conbime into temp query statement
    new_value = '"{}"'.format("\", \"".join(new_values))
    temp_table_stmt = "WITH {} as (SELECT {}, regexp_multi_match({}, json_parse('[{}]')) AS bitstr FROM {})".format(temp_table_name, colume_name, new_key[0], new_value, table_name)

    # combine back_stmt
    new_conn_2 = [' OR '.join(new_conn)]
    
    # hardcoded
    new_front_stmt = "SELECT COUNT(distinct {}) FROM {}".format(colume_name,temp_table_name)
    
    # combine all statements
    new_stmt = "{} \n{} WHERE {} \n;\n".format(temp_table_stmt, new_front_stmt, new_conn_2[0])
    return new_stmt

def main():
    convert_obj = get_opt()
    sql_stmts = read_sql_statements(convert_obj.filename)
    converted_stmts = convert_statements(sql_stmts, convert_obj.target_service, convert_obj.query_type)
    write_output_statement(convert_obj.output_file, converted_stmts, convert_obj.verbose)

if __name__ == "__main__":
    main()