import re
import os
import psycopg2
import xmlschema
import xml.etree.ElementTree as etree
from lxml import etree as ltree

def parseXsd(directory):
    """
    Парсинг XSD файлов
    Принимает на вход папку с XSD файлами, разбирает их по очереди
    и на выходе выдает словарь вида:
    '<ИМЯ ФАЙЛА СХЕМЫ И СООТВЕТСТВУЮЩЕГО ЕЙ XML>': {
        'tableName': <ИМЯ ТАБЛИЦЫ>,
        'fields': {
            <ИМЯ ПОЛЯ>: {
                'name': '<ИМЯ ПОЛЯ>',
                'type': '<ТИП ПОЛЯ>,
                'length': '<ДЛИНА ПОЛЯ>' (опционально)
            },
            ...
        },
        'object': <объект XMLschema>
    }
    """
    parsedXSD = {}
    # for xsd in os.listdir(directory.replace('\\', '\\\\')):
    for xsd in os.listdir(directory):
        # schema = open(directory.replace('\\', '\\\\') + '\\\\' + xsd, 'r', encoding='utf_8_sig')
        schema = open(directory + '/' + xsd, 'r', encoding='utf_8_sig')
        schemadict = {}
        schemadict['object'] = xmlschema.XMLSchema(directory + '/' + xsd)
        schemastr = schema.read()

        def extractContent(xsd):
            """
            Извлекает полезное содержимое из XSD файла
            Отбрасывает преамбулу, убирает все переносы строк и табуляции
            """
            string = xsd.replace('\n', '').replace('\t', '')
            return string[string.find('<xs:', 0):]

        openTags = []

        def diver(string, dictionary, filename, openTags=openTags):
            """
            Конвертирует XSD в словарь
            Принимает конвертированный в строку XSD, словарь, в который будут добавляться данные из XSD,
            имя xsd файла, вспомогательный массив, в который заносятся открытые теги.
            """
            filename = filename
            if len(string) < 2:
                return 'Search complete'

            if string.find('<xs:') != -1 and string.find('</xs') != -1 and string.find('<xs:') < string.find('</xs'):
                openTagStartIndex = string.find('<xs:')
                openTagNameWithAttributes = string[openTagStartIndex:string.find('>') + 1]
                openTagNameClear = openTagNameWithAttributes[4:re.search(r'\ |>', openTagNameWithAttributes).start()]
                openTags.append(openTagNameClear)

                if openTagNameClear == 'element' and openTagNameWithAttributes.find('maxOccurs') == -1:
                    if openTagNameWithAttributes[
                       openTagNameWithAttributes.find(' name=') + 7:openTagNameWithAttributes.find('"',
                                                                                                   openTagNameWithAttributes.find(
                                                                                                           ' name=') + 8)] == 'ITEMS':
                        dictionary['tableName'] = filename[3:filename.find('_2')]
                    else:
                        dictionary['tableName'] = openTagNameWithAttributes[openTagNameWithAttributes.find(
                            ' name=') + 7:openTagNameWithAttributes.find('"',
                                                                         openTagNameWithAttributes.find(' name=') + 8)]
                    dictionary['fields'] = []

                if openTagNameClear == 'attribute':
                    tempDict = {}
                    tempDict['name'] = openTagNameWithAttributes[
                                       openTagNameWithAttributes.find(' name=') + 7:openTagNameWithAttributes.find('"',
                                                                                                                   openTagNameWithAttributes.find(
                                                                                                                       ' name=') + 8)]
                    dictionary['fields'].append(tempDict)

                if openTagNameClear == 'restriction':
                    if openTagNameWithAttributes[
                       openTagNameWithAttributes.find(' base=') + 10:openTagNameWithAttributes.find('"',
                                                                                                    openTagNameWithAttributes.find(
                                                                                                            ' base=') + 8)] == 'string':
                        dictionary['fields'][-1]['type'] = 'character varying'
                    elif openTagNameWithAttributes[
                         openTagNameWithAttributes.find(' base=') + 10:openTagNameWithAttributes.find('"',
                                                                                                      openTagNameWithAttributes.find(
                                                                                                              ' base=') + 8)] == 'integer' or openTagNameWithAttributes[
                                                                                                                                              openTagNameWithAttributes.find(
                                                                                                                                                      ' base=') + 10:openTagNameWithAttributes.find(
                                                                                                                                                      '"',
                                                                                                                                                      openTagNameWithAttributes.find(
                                                                                                                                                              ' base=') + 8)] == 'int':
                        dictionary['fields'][-1]['type'] = 'bigint'
                    else:
                        dictionary['fields'][-1]['type'] = openTagNameWithAttributes[openTagNameWithAttributes.find(
                            ' base=') + 10:openTagNameWithAttributes.find('"',
                                                                          openTagNameWithAttributes.find(' base=') + 8)]

                if openTagNameClear == 'totalDigits' or openTagNameClear == 'length' or openTagNameClear == 'maxLength':
                    dictionary['fields'][-1]['length'] = int(openTagNameWithAttributes[openTagNameWithAttributes.find(
                        ' value=') + 8:openTagNameWithAttributes.find('"',
                                                                      openTagNameWithAttributes.find(' value=') + 8)])

                if openTagNameWithAttributes.find('type') != -1:
                    dictionary['fields'][-1]['type'] = openTagNameWithAttributes[openTagNameWithAttributes.find(
                        ' type=') + 10:openTagNameWithAttributes.find('"',
                                                                      openTagNameWithAttributes.find(' type=') + 8)]

                if openTagNameWithAttributes[-2] == '/':
                    openTags.pop()
                diver(string[openTagStartIndex + len(openTagNameWithAttributes):], dictionary, filename)
            else:
                openTags.pop()
                closeTagStartIndex = string.find('</xs:')
                closeTagName = string[closeTagStartIndex:string.find('>') + 1]
                diver(string[closeTagStartIndex + len(closeTagName):], dictionary, filename)

        string = extractContent(schemastr)
        diver(string, schemadict, xsd)
        for field in schemadict['fields']:
            if 'type' not in field or field['type'] == 'integer' or field['type'] == 'int':
                field['type'] = 'bigint'

        parsedXSD[xsd[3:xsd.find('_2')]] = schemadict

        schema.close()
    return parsedXSD

def createPgTables(directory, parsedXSD, conn, cursor):
    """
    Создание таблиц в БД PostgreSQL
    Функция принимает директорию с XML, массив разобранных XSD файлов, конвертированных в словари,
    объект соединения с БД и объект курсора.
    Создает из каждого XSD отдельную таблицу в БД.
    """
    for ntbl in parsedXSD:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS public.{0} ({1} {2} CONSTRAINT {0}_{1}_pk PRIMARY KEY);".format(
                ntbl, parsedXSD[ntbl]['fields'][0]['name'], parsedXSD[ntbl]['fields'][0]['name']
            )
        )


    for item in os.listdir(directory):
        if item.find('.XML') != -1:
            if item[3:item.find('_2')] in list(parsedXSD.keys()):
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS fiastest.{0} ({1} bigint CONSTRAINT {0}_{1}_pk PRIMARY KEY);".format(
                        item[3:item.find('_2')], parsedXSD[item[3:item.find('_2')]]['fields'][0]['name'].lower()))

                conn.commit()
                for field in parsedXSD[item[3:item.find('_2')]]['fields'][1:]:
                    if field['type'] == 'character varying':
                        cursor.execute(
                            "ALTER TABLE fiastest.{0} ADD COLUMN \"{1}\" {2}({3});".format(item[3:item.find('_2')],
                                                                                           field['name'].lower(),
                                                                                           field['type'],
                                                                                           field['length']))
                    if field['type'] == 'date' or field['type'] == 'boolean' or field['type'] == 'bigint':
                        cursor.execute(
                            "ALTER TABLE fiastest.{0} ADD COLUMN \"{1}\" {2};".format(item[3:item.find('_2')],
                                                                                      field['name'].lower(),
                                                                                      field['type']))

            if item.find('PARAMS_2') != -1:
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS fiastest.{0} ({1} bigint CONSTRAINT {0}_{1}_pk PRIMARY KEY);".format(
                        item[3:item.find('_2')], parsedXSD['PARAM']['fields'][0]['name'].lower()))
                conn.commit()
                for field in parsedXSD['PARAM']['fields'][1:]:
                    if field['type'] == 'character varying':
                        cursor.execute(
                            "ALTER TABLE fiastest.{0} ADD COLUMN \"{1}\" {2}({3});".format(item[3:item.find('_2')],
                                                                                           field['name'].lower(),
                                                                                           field['type'],
                                                                                           field['length']))
                    if field['type'] == 'date' or field['type'] == 'boolean' or field['type'] == 'bigint':
                        cursor.execute(
                            "ALTER TABLE fiastest.{0} ADD COLUMN \"{1}\" {2};".format(item[3:item.find('_2')],
                                                                                      field['name'].lower(),
                                                                                      field['type']))
        if item == '30':
            # createPgTables(directory + '\\' + item, parsedXSD, conn, cursor)
            createPgTables(directory + '/' + item, parsedXSD, conn, cursor)

        conn.commit()


def findElementNode(root, i):
    elementNode = None;
    for child in root.getchildren():
        if (child.tag.find('element') > 0):
            i = i + 1
        if (i == 2):
            elementNode = child;
            return elementNode;
        else:
            elementNode = findElementNode(child, i);
    return elementNode;

def findNode(root, nodeName):
    elementNode = None;
    for child in root.getchildren():
        if (child.tag.find(nodeName) > 0):
            elementNode = child;
            return elementNode;
        else:
            elementNode = findNode(child, nodeName);
    return elementNode;

def getIntegerType(restriction):
    if (restriction != None):
        totalDigitsNode = findNode(restriction, 'totalDigits')
        enumerationNode = findNode(restriction, 'enumeration')
        if (totalDigitsNode != None):
            intSize = int(totalDigitsNode.get('value'))
            if (intSize > 9):
                return 'bigint'
            else:
                return 'integer'
        elif (enumerationNode != None):
            return 'smallint'
        else:
            return 'bigint'
    else:
        return 'bigint'

def getStringType(restriction):
    if (restriction != None) :
        lengthNode = findNode(restriction, 'length')
        maxLengthNode = findNode(restriction, 'maxLength')
        if (lengthNode != None):
            length = lengthNode.get('value')
            return 'varchar('+ length + ')'
        elif (maxLengthNode != None):
            length = maxLengthNode.get('value')
            return 'varchar('+ length + ')'
        else:
            return 'text'
    else:
        return 'text'

def getType(simpleTypeNode, typeName):
    restriction = None;
    if typeName == '':
        restriction = findNode(simpleTypeNode, 'restriction')
        typeName = restriction.get('base')

    if (typeName == 'xs:integer' or typeName == 'xs:int'):
        type = getIntegerType(restriction)
    elif (typeName == 'xs:long'):
        type = 'bigint'
    elif (typeName == 'xs:byte'):
        type = 'smallint'
    elif (typeName == 'xs:string'):
        type = getStringType(restriction)
    elif (typeName == 'xs:date'):
        type = 'timestamp'
    elif (typeName == 'xs:boolean'):
        type = 'boolean'

    return type

def queryAddColumns(complexTypeNode, tableName):
    queries = []
    reservedWords = ['desc']
    for child in complexTypeNode:
        if (child.tag.find('attribute') > 0):
            columnName = child.get('name').lower()
            if (reservedWords.count(columnName)):
                columnName = '\"' + columnName + '\"'
            use = child.get('use')
            typeName = child.get('type')
            if (typeName == None):
                simpleTypeNode = findNode(child, 'simpleType')
                # В xsd PARAMS забыли указать тип колонке objectid
                if (simpleTypeNode == None and columnName == 'objectid'):
                    type = 'bigint'
                else:
                    type = getType(simpleTypeNode, '')
            else:
                type = getType(None, typeName)
            # query = 'alter table ' + tableName + ' add column  if not exists ' + columnName + ' ' \
            #         + type + (' not null' if use == 'required' else '') + ';'

            query = 'alter table ' + tableName + ' add column  if not exists ' + columnName + ' ' \
                        + type + ';'
            queries.append(query)
    return queries

def createTablesFromXSD(directory):
    repeatedNames = ['item']
    for xsd in os.listdir(directory):
        tree = ltree.parse(directory + '/' + xsd)
        root = tree.getroot()
        elementNode = findElementNode(root, 0);
        tableName = elementNode.attrib['name'].lower()

        if (repeatedNames.count(tableName) > 0):
            tableName = xsd[3:xsd.find('_2')].lower()+ '_' + tableName

        queryCreateTable = 'CREATE TABLE IF NOT EXISTS ' + tableName + '();'
        complexTypeNode = findNode(elementNode, 'complexType')
        queries = queryAddColumns(complexTypeNode, tableName)
        cursor.execute(queryCreateTable)
        for query in queries:
            cursor.execute(query)
        conn.commit()
        i = 0


try:
    conn = psycopg2.connect("dbname=fias user=postgres password=postgres host=localhost")
except psycopg2.Error as e:
    print(e)

cursor = conn.cursor()

conn.commit()

directoryXML = '/media/sf_gly/FIAS/GAR/gar_delta_xml'
directoryXSD = '/media/sf_gly/FIAS/gar_xsd'

parsedXSD = createTablesFromXSD(directoryXSD)