import re
import os
import psycopg2
import xmlschema
import xml.etree.ElementTree as etree
from lxml import etree as ltree


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
