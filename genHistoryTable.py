import logging
import sqlparse
import os

logging.basicConfig(level= logging.INFO, 
                    filename='genHistoryTable.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

root_directory = '.\\input\\'

triggerTypes = {
                'INSERT': 'NEW',
                'UPDATE': 'NEW',
                'DELETE': 'OLD'
                }

def parseDDL(filename): 
    data = ""
    logging.debug(f'Opening {filename}...')
    with open(filename, 'r') as f:
        data = f.read()
    sqldata = sqlparse.format(data, 
                              keyword_case = 'upper',
                              identifier_case = 'upper',
                              strip_comments = True,
                              reindent = False,
                              use_space_around_operators = True,
                              indent_tabs = False)
    datalist = sqldata.split()

    columns = []

    if datalist[0] == 'CREATE' and datalist[1] == 'TABLE':
        table = datalist[2].split('.')
        if len(table) != 2:
            logging.error('Invalid schema/table name')
            return -1
        else:
            schema = table[0].strip('"')
            tablename = table[1].strip('"')
            logging.info(f'Found schema.table name: {schema}.{tablename}')
            readColumns = sqldata[sqldata.find('(') + 1:].lstrip()
            eolindex = readColumns.find(',\n')
            while eolindex != -1:
                column = readColumns[:eolindex].strip('\n')
                logging.debug(column)
                column = column.split()
                columnName = column[0].strip('"')
                if column[1].find('(') > 0 and column[1].find(')') == -1:
                    columnType = column[1] + ' ' + column[2]
                    index = 3
                else:
                    columnType = column[1]
                    index = 2
                columnOptions = ""
                if len(column) > index:
                    while index < len(column):
                        columnOptions = columnOptions + column[index] + ' ' 
                        index+=1
                    columnOptions = columnOptions.rstrip()
                columns.append((columnName, columnType, columnOptions))
                readColumns = readColumns[eolindex + 1:]
                eolindex = readColumns.find(',\n')
            # Capture final column
            eolindex = readColumns.find(' )')
            column = readColumns[:eolindex].strip('\n')
            logging.debug(column)
            column = column.split()
            columnName = column[0].strip('"')
            if column[1].find('(') > 0 and column[1].find(')') == -1:
                columnType = column[1] + ' ' + column[2]
                index = 3
            else:
                columnType = column[1]
                index = 2
            columnOptions = ""
            if len(column) > index:
                while index < len(column):
                    columnOptions = columnOptions + column[index] + ' ' 
                    index+=1
                columnOptions = columnOptions.rstrip()
            columns.append((columnName, columnType, columnOptions))
            # ReadColumn remnants should just be properties and anything else to EOF (comments, grants, etc.)
            remainder = readColumns[eolindex + 2 : readColumns.find(';') + 1].lstrip('\n').lstrip()
            logging.debug(columns)
            logging.debug(remainder)
            return (schema, tablename, columns, remainder)
    else:
        logging.error(f'File begins with {datalist[0]} {datalist[1]}')
        return -1

def buildHTable(schema, tablename, columns, remainder):
    columnsFormatted = ""
    for i in columns:
        columnsFormatted = columnsFormatted + f'\t{i[0]} {i[1]},\n'
    columnsFormatted = columnsFormatted.rstrip(',\n')
    logging.debug(columnsFormatted)
    logging.info(f'Writing H_{tablename} to file')
    toWrite = f'CREATE TABLE {schema}.H_{tablename}\n' \
            '(\n' \
            '\tHIST_ID NUMBER PRIMARY KEY,\n' \
            '\tCHANGE VARCHAR(10) NOT NULL,\n' \
            f'{columnsFormatted}\n' \
            ')\n' \
            f'{remainder}' \
            '\n/'
    directory = '.\\output\\TABLES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}H_{tablename}.sql', 'w') as f:
        f.write(toWrite)
    logging.info(f'Writing H_{tablename}_SEQ to file')
    toWrite = f'CREATE SEQUENCE {schema}.H_{tablename}_SEQ' \
            '\n\tMINVALUE 1' \
            '\n\tNOMAXVALUE' \
            '\n\tINCREMENT BY 1' \
            '\n\tSTART WITH 1' \
            '\n\tCACHE 20' \
            '\n\tNOORDER' \
            '\n\tNOCYCLE' \
            '\n\tNOKEEP' \
            '\n\tNOSCALE;' \
            '\n/'
    directory = '.\\output\\SEQUENCES\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}H_{tablename}_SEQ.sql', 'w') as f:
        f.write(toWrite)
    logging.info(f'Writing H_{tablename}_TRG to file')
    toWrite = f'CREATE OR REPLACE EDITIONABLE TRIGGER {schema}.H_{tablename}_TRG' \
            '\nBEFORE INSERT' \
            f'\nON {schema}.H_{tablename}' \
            '\nREFERENCING NEW AS NEW OLD AS OLD' \
            '\nFOR EACH ROW' \
            '\nBEGIN' \
            f'\n\t:new.HIST_ID := {schema}.H_{tablename}_SEQ.nextval;' \
            f'\nEND H_{tablename}_TRG;' \
            '\n/' \
            f'\nALTER TRIGGER {schema}.H_{tablename}_TRG ENABLE;' \
            '\n/'
    directory = '.\\output\\TRIGGERS\\'
    os.makedirs(directory, exist_ok = True)
    with open(f'{directory}H_{tablename}_TRG.sql', 'w') as f:
        f.write(toWrite)


def buildHTriggers(schema, tablename, columns):
    directory = '.\\output\\TRIGGERS\\'
    os.makedirs(directory, exist_ok = True)
    for key in triggerTypes.keys():
        columnsFormatted = '\tCHANGE,\n'
        for i in columns:
            columnsFormatted = columnsFormatted + f'\t{i[0]},\n'
        columnsFormatted = columnsFormatted.rstrip(',\n')
        valuesFormatted = f'\t\'{key}\',\n'
        for i in columns:
            valuesFormatted = valuesFormatted + f'\t:{triggerTypes[key]}.{i[0]},\n'
        valuesFormatted = valuesFormatted.rstrip(',\n')
        logging.debug(columnsFormatted)
        logging.debug(valuesFormatted)
        logging.info(f'Writing {tablename}_H_{key[0:3]}_TRG to file')
        toWrite = f'CREATE OR REPLACE EDITIONABLE TRIGGER {schema}.{tablename}_H_{key[0:3]}_TRG' \
                f'\nBEFORE {key}' \
                f'\nON {schema}.{tablename}' \
                '\nREFERENCING NEW AS NEW OLD AS OLD' \
                '\nFOR EACH ROW' \
                '\nBEGIN' \
                f'\nINSERT INTO {schema}.H_{tablename}' \
                '\n(' \
                f'\n{columnsFormatted}' \
                '\n)' \
                '\nVALUES' \
                '\n(' \
                f'\n{valuesFormatted}' \
                '\n);' \
                f'\nEND {tablename}_H_{key[0:3]}_TRG;' \
                '\n/' \
                f'\nALTER TRIGGER {schema}.{tablename}_H_{key[0:3]}_TRG ENABLE;' \
                '\n/'
        with open(f'{directory}{tablename}_H_{key[0:3]}_TRG.sql', 'w') as f:
            f.write(toWrite)

if __name__ == '__main__':
    logging.info('Start application')
    os.makedirs(root_directory, exist_ok = True)
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            table = parseDDL(os.path.join(root, file))
            if table != -1:
                buildHTable(table[0], table[1], table[2], table[3])
                buildHTriggers(table[0], table[1], table[2])
            else:
                logging.error(f'Unable to parse script {file}')