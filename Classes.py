"""
TreeSpark Classes

Author: Paula López
Date: Oct, 2021
Site: https://gitlab.com/Danikawaii/treespark

"""


from pyspark.sql.types import *
from pyspark.sql import Row as Row
from functools import reduce
import datetime as dd
import inspect


def getFieldValue(row, key):
    return row[key]


def motherExists(row, motherKey, motherEP):
    return row[motherKey] != motherEP


def mother_motherExists(row):
    if row["mother_motherExists"] == True or row["mother_motherExists"] == 'true':
        return True
    else:
        return False


def childrenCount(row):
    return row["children_count"]


def mother_childrenCount(row):
    return row["mother_childrenCount"]


def childrenOrder(row, birthOrderKey):
    return row[birthOrderKey]


def mother_childrenOrder(row):
    return row['mother_childrenOrder']


def nextSister(row):
    if row["nextSister"] == True or row["nextSister"] == 'true':
        return True
    else:
        return False


def mother_nextSister(row):
    if row["mother_nextSister"] == True or row["mother_nextSister"] == 'true':
        return True
    else:
        return False


def previousSister(row):
    if row["previousSister"] == True or row["previousSister"] == 'true':
        return True
    else:
        return False


def mother_previousSister(row):
    if row["mother_previousSister"] == True or row["mother_previousSister"] == 'true':
        return True
    else:
        return False


def sistersCount(row):
    return row["sisters_count"]


def mother_sistersCount(row):
    return row["mother_sistersCount"]


def granddaughterCount(row):
    return row["granddaughter_count"]


def mother_granddaughterCount(row):
    return row["mother_granddaughterCount"]


class Kind():
    def __init__(self, tipo, key, base, columnas, nombreBase, datum):
        self.base = base
        self.columnas = columnas
        self.nombreBase = nombreBase
        self.datum = datum
        self.tipo = []
        self.key = []
        self.tipo.append(tipo)
        self.key.append(key)

    def getKind(self):
        return self.tipo

    def getKey(self):
        return self.key

    def addKind(self, tipo):
        self.tipo.append(tipo)
        return self

    def addKey(self, key):
        self.key.append(key)
        return self

    def addKindAndKey(self, tipo, key):
        self.addKind(tipo)
        self.addKey(key)
        return self

    def __getitem__(self, key):
        if key == self.nombreBase:  # is the individuos table
            self.addKindAndKey(-1, self.nombreBase)
            return self
        else:
            if key in self.datum:  # ind["CONTROLES"] IS AN ANNEXED TABLE
                self.addKindAndKey(0, key)
                return self
            else:
                if key in self.columnas:  # is in the base, is a field
                    self.addKindAndKey(1, key)
                    return self
                else:
                    for nombreTabla in self.datum:  # is not in the base and it is a field
                        if key in self.datum[nombreTabla][2]:  # in the columns
                            self.addKindAndKey(2, key)
                            return self
        self.addKindAndKey(3, '')
        return self


class Ind():
    def __init__(self, base, idKey, columnas, nombreBase, datum):
        self.base = base
        self.idKey = idKey
        self.columnas = columnas
        self.nombreBase = nombreBase
        self.datum = datum

    def __getitem__(self, key):
        if key == self.nombreBase:  # is the individuos table
            return Kind(-1, self.nombreBase, self.base, self.columnas, self.nombreBase, self.datum)
        else:
            if key in self.datum:  # ind["CONTROLES"] IS AN ANNEXED TABLE
                return Kind(0, key, self.base, self.columnas, self.nombreBase, self.datum)
            else:
                if key in self.columnas:  # is in base, is a field
                    return Kind(1, key, self.base, self.columnas, self.nombreBase, self.datum)
                else:
                    for nombreTabla in self.datum:  # is not in base and it is a field
                        if key in self.datum[nombreTabla][2]:  # in the columns
                            return Kind(2, {'tabla': nombreTabla, 'campo': key}, self.base, self.columnas, self.nombreBase, self.datum)
        return Kind(3, '', self.base, self.columnas, self.nombreBase, self.datum)

    def processFunction(self, func, cod=None):
        if func[0] == '[':
            s = 'self'+func
            temp = eval(s)  # return the KIND object
            tipos = temp.getKind()
            keys = temp.getKey()
            if cod == 'M':  # ind mother process
                # if the last element is a field
                if tipos[len(tipos)-1] == 1 or tipos[len(tipos)-1] == 2:
                    tasks = self.parseKeysOfMother(tipos, keys, 1)
                else:  # tables
                    tasks = self.parseKeysOfMother(tipos, keys, 0)
            else:
                # if the last element is a field
                if tipos[len(tipos)-1] == 1 or tipos[len(tipos)-1] == 2:
                    tasks = self.parseKeys(tipos, keys, 1)
                else:  # tables
                    tasks = self.parseKeys(tipos, keys, 0)
            resu = tasks
        else:
            s = 'self.'+func+'()'
            resu = eval(s)
        return resu

    def motherExists(self):
        return {'k': 'comun', 'data': 'motherExists(row,motherKey,motherEP)'}

    def childrenOrder(self):
        return {'k': 'comun', 'data': 'childrenOrder(row,birthOrderKey)'}

    def childrenCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t +
                    '.rdd).map(lambda row:(row[motherKey],Row(children_count=row[1]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('childrenCount(row)')
        return {'k': 'compleja', 'data': task}

    def hasNextSister(self):
        task = []
        t = '((((self.base.rdd).map(lambda row:(row[motherKey],row))).join((self.base.rdd).map(lambda row:(row[motherKey],row)))).map(lambda row: (row[1][0],row[1][1][birthOrderKey] - row[1][0][birthOrderKey] == 1)))'
        true = t+'.filter(lambda row: row[1]==True).map(lambda row: row[0])'
        false = t+'.filter(lambda row: row[1]==False).map(lambda row: row[0])'
        s = false+'.subtract('+true+').distinct().map(lambda row: (row.asDict(),Row(nextSister=False).asDict())).union(' + \
            true + \
            '.map(lambda row: (row.asDict(),Row(nextSister=True).asDict())))'
        task.append(
            s+'.map(lambda x: {**x[0],**x[1]}).map(lambda x: (Row(**x)))')
        task.append('nextSister(row)')
        return {'k': 'compleja', 'data': task}

    def tieneHermanaPrevia(self):
        task = []
        t = '((((self.base.rdd).map(lambda row:(row[motherKey],row))).join((self.base.rdd).map(lambda row:(row[motherKey],row)))).map(lambda row: (row[1][0],row[1][0][birthOrderKey] - row[1][1][birthOrderKey] == 1)))'
        true = t+'.filter(lambda row: row[1]==True).map(lambda row: row[0])'
        false = t+'.filter(lambda row: row[1]==False).map(lambda row: row[0])'
        s = false+'.subtract('+true+').distinct().map(lambda row: (row.asDict(),Row(previousSister=False).asDict())).union(' + \
            true + \
            '.map(lambda row: (row.asDict(),Row(previousSister=True).asDict())))'
        task.append(
            s+'.map(lambda x: {**x[0],**x[1]}).map(lambda x: (Row(**x)))')
        task.append('previousSister(row)')
        return {'k': 'compleja', 'data': task}

    def sistersCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t +
                    '.rdd).map(lambda row:(row[motherKey],Row(sisters_count=row[1]-1).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('sistersCount(row)')
        return {'k': 'compleja', 'data': task}

    def granddaughterCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row))).join((' + \
            t+'.rdd).map(lambda row:(row[motherKey],row))))'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row))).join(' + \
            t+'.map(lambda row: (row[1][0][motherKey],row[1][1]["count"]))))'
        t = t + \
            '.map(lambda row: (row[0],row[1][1])).reduceByKey(lambda x,y: x+y).map(lambda row: (row[0],Row(granddaughter_count=row[1]).asDict()))'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(' + \
            t+')).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))'
        task.append(t)
        task.append('granddaughterCount(row)')
        return {'k': 'compleja', 'data': task}

    def mother_motherExists(self):
        task = []
        t = '(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join((self.base.rdd).map(lambda row:(row[idKey],Row(mother_motherExists=(row[motherKey]!=motherEP)).asDict()))))'
        task.append(
            t+'.map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_motherExists(row)')
        return {'k': 'compleja', 'data': task}

    def mother_childrenOrder(self):
        task = []
        t = '(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join((self.base.rdd).map(lambda row:(row[idKey],Row(mother_childrenOrder=row[birthOrderKey]).asDict()))))'
        task.append(
            t+'.map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_childrenOrder(row)')
        return {'k': 'compleja', 'data': task}

    def mother_childrenCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        t = '((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t + \
            '.rdd).map(lambda row:(row[motherKey],Row(mother_childrenCount=row[1]).asDict())))'
        task.append(
            t+'.map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_childrenCount(row)')
        return {'k': 'compleja', 'data': task}

    def mother_hasNextSister(self):
        task = []
        t = '((((self.base.rdd).map(lambda row:(row[motherKey],row))).join((self.base.rdd).map(lambda row:(row[motherKey],row)))).map(lambda row: (row[1][0][idKey],row[1][1][birthOrderKey] - row[1][0][birthOrderKey] == 1)))'
        true = t+'.filter(lambda row: row[1]==True).map(lambda row: row[0])'
        false = t+'.filter(lambda row: row[1]==False).map(lambda row: row[0])'
        s = false+'.subtract('+true+').distinct().map(lambda row: (row,Row(mother_nextSister=False).asDict())).union(' + \
            true + \
            '.map(lambda row: (row,Row(mother_nextSister=True).asDict())))'
        t = '(self.base.rdd).map(lambda row:(row[motherKey],row.asDict())).join('+s+')'
        task.append(
            t+'.map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_nextSister(row)')
        return {'k': 'compleja', 'data': task}

    def mother_tieneHermanaPrevia(self):
        task = []
        t = '((((self.base.rdd).map(lambda row:(row[motherKey],row))).join((self.base.rdd).map(lambda row:(row[motherKey],row)))).map(lambda row: (row[1][0][idKey],row[1][0][birthOrderKey] - row[1][1][birthOrderKey] == 1)))'
        true = t+'.filter(lambda row: row[1]==True).map(lambda row: row[0])'
        false = t+'.filter(lambda row: row[1]==False).map(lambda row: row[0])'
        s = false+'.subtract('+true+').distinct().map(lambda row: (row,Row(mother_previousSister=False).asDict())).union(' + \
            true+'.map(lambda row: (row,Row(mother_previousSister=True).asDict())))'
        t = '(self.base.rdd).map(lambda row:(row[motherKey],row.asDict())).join('+s+')'
        task.append(
            t+'.map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_previousSister(row)')
        return {'k': 'compleja', 'data': task}

    def mother_sistersCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        t = '(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t + \
            '.rdd).map(lambda row:(row[motherKey],Row(mother_sistersCount=row[1]-1).asDict())))).map(lambda row: (row[1][0][idKey],row[1][1]))'
        task.append('(self.base.rdd).map(lambda row:(row[motherKey],row.asDict())).join(' +
                    t+').map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_sistersCount(row)')
        return {'k': 'compleja', 'data': task}

    def mother_granddaughterCount(self):
        task = []
        t = '((self.base).groupBy(motherKey).count())'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row))).join((' + \
            t+'.rdd).map(lambda row:(row[motherKey],row))))'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row))).join(' + \
            t+'.map(lambda row: (row[1][0][motherKey],row[1][1]["count"]))))'
        t = t + \
            '.map(lambda row: (row[0],row[1][1])).reduceByKey(lambda x,y: x+y).map(lambda row: (row[0],Row(mother_granddaughterCount=row[1]).asDict()))'
        t = '(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(' + \
            t+')).map(lambda row: (row[1][0][idKey],row[1][1]))'
        task.append('(self.base.rdd).map(lambda row:(row[motherKey],row.asDict())).join(' +
                    t+').map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
        task.append('mother_granddaughterCount(row)')
        return {'k': 'compleja', 'data': task}

#######################################################################################
    def parseKeys(self, tipos, keys, tipoParseo):
        if tipoParseo == 0:  # TABLES
            lista = []
            indice = 0
            for tipo in tipos:
                if tipo == -1:
                    lista.append('self.base')
                elif tipo == 0:
                    lista.append('self.datum["'+keys[indice]+'"]')
                indice += 1
            if len(lista) > 1:  # are two or more tables
                if len(lista) > 2:  # more than two tables
                    task = []
                    if (tipos[0] == 0):  # annexed
                        t = 'self.renameTables("' + \
                            keys[0]+'","'+keys[1]+'")'
                        tamanioKeys = len(keys)
                        campo = keys[0]+'_'+keys[1]
                        agrupar = lista[0]+'[1],'+lista[1] + \
                            '[3]["'+keys[0]+'"][1]'
                        for i in range(2, tamanioKeys):  # nest joins
                            campo += '_'+keys[i]
                            agrupar += ',"'+keys[i-1]+'_"+' + \
                                lista[i]+'[3]["'+keys[i-1]+'"][1]'
                            t = 'self.renameTables("JOIN_ESPECIAL","' + \
                                keys[i]+'",['+t+',"'+keys[i-1]+'"])'
                        # ID,NRO_PROD,CONTROLES_NRO_DATITO
                        t = '('+t+').groupBy(['+agrupar+']).count()'
                        idAnexa = str(self.datum[keys[0]][1])
                        fila = 'Row('
                        for i in range(1, tamanioKeys):
                            fila += str(self.datum[keys[i]][3]
                                        [keys[i-1]][1])+'=row['+str(i)+'],'
                        fila += campo+'_count=row['+str(tamanioKeys)+']'
                        task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row.' +
                                    idAnexa+','+fila+').asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append('getFieldValue(row,"'+campo+'_count")')
                    else:
                        t = 'self.renameTables("' + \
                            keys[0]+'","'+keys[1]+'")'
                        tamanioKeys = len(keys)
                        campo = keys[0]+'_'+keys[1]
                        agrupar = '"'+self.idKey+'"'
                        for i in range(2, tamanioKeys):  # nest joins
                            campo += '_'+keys[i]
                            agrupar += ',"'+keys[i-1]+'_"+' + \
                                lista[i]+'[3]["'+keys[i-1]+'"][1]'
                            t = 'self.renameTables("JOIN_ESPECIAL","' + \
                                keys[i]+'",['+t+',"'+keys[i-1]+'"])'
                        # ID,NRO_PROD,CONTROLES_NRO_DATITO
                        t = '('+t+').groupBy(['+agrupar+']).count()'
                        fila = 'Row('
                        for i in range(2, tamanioKeys):
                            fila += str(self.datum[keys[i]][3]
                                        [keys[i-1]][1])+'=row['+str(i-1)+'],'
                        fila += campo+'_count=row['+str(tamanioKeys-1)+'])'
                        task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t +
                                    '.rdd).map(lambda row:(row[idKey],'+fila+'.asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append('getFieldValue(row,"'+campo+'_count")')
                else:  # two tables
                    if (tipos[0] == -1 and tipos[1] == 0):  # the base and the annexed table
                        task = []
                        idAnexa = str(self.datum[keys[1]][1])
                        t = '('+lista[1]+'[0].groupBy(' + \
                            lista[1]+'[1]).count())'
                        task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row["'+idAnexa +
                                    '"],Row('+keys[1]+'_count=row[1]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append('getFieldValue(row,"'+keys[1]+'_count")')
                    else:
                        if (tipos[0] == 0 and tipos[1] == 0):  # Two annexed tables
                            task = []
                            idAnexa = str(self.datum[keys[1]][1])
                            s = self.datum[keys[1]][3][keys[0]][1]
                            # One of the annexed tables
                            t = 'self.renameTables("' + \
                                keys[0]+'","'+keys[1]+'")'
                            # ID,NRO_PROD
                            t = '('+t+').groupBy(['+lista[1]+'[1],' + \
                                lista[1]+'[3]["'+keys[0]+'"][1]]).count()'
                            task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row.'+idAnexa+',Row(' +
                                        s+'=row[1],'+keys[0]+'_'+keys[1]+'_count=row[2]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                            task.append(
                                'getFieldValue(row,"'+keys[0]+'_'+keys[1]+'_count")')
                t = {'k': 'compleja', 'data': task}
            else:  # ind["CONTROLES"] tabla anexa
                if tipos[0] == 0:  # tabla anexa
                    task = []
                    idAnexa = str(self.datum[keys[0]][1])
                    t = '('+lista[0]+'[0].groupBy('+lista[0]+'[1]).count())'
                    task.append('(((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row["'+idAnexa +
                                '"],Row('+keys[0]+'_count=row[1]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"'+keys[0]+'_count")')
                    t = {'k': 'compleja', 'data': task}
        else:
            if len(keys) > 1:  # ind["OTROS"]["DATITO"] tabla/s y campo
                if (tipos[0] == -1):  # the base
                    t = {'k': 'comun',
                         'data': 'getFieldValue(row,"'+keys[1]+'")'}
                elif (tipos[0] == 0):  # # annexed table
                    task = []
                    idAnexa = str(self.datum[keys[0]][1])
                    task.append('((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(((self.datum["'+keys[0]+'"][0].rdd)).map(lambda row:(row["' +
                                idAnexa+'"],Row('+keys[1]+'=row["'+keys[1]+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"'+keys[1]+'")')
                    t = {'k': 'compleja', 'data': task}
            else:  # just the field
                if tipos[0] == 1:  # is a field of the base
                    t = {'k': 'comun',
                         'data': 'getFieldValue(row,"'+keys[0]+'")'}
                elif tipos[0] == 2:  # is a field of an annex
                    task = []
                    idAnexa = str(self.datum[keys[0]['tabla']][1])
                    task.append('((self.base.rdd).map(lambda row:(row[idKey],row.asDict()))).join(((self.datum["'+keys[0]['tabla']+'"][0].rdd)).map(lambda row:(row["' +
                                idAnexa+'"],Row('+keys[0]['campo']+'=row["'+keys[0]['campo']+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"'+keys[0]['campo']+'")')
                    t = {'k': 'compleja', 'data': task}
        return t
################################################################################

    def parseKeysOfMother(self, tipos, keys, tipoParseo):
        if tipoParseo == 0:  # TABLAS
            lista = []
            indice = 0
            for tipo in tipos:
                if tipo == -1:
                    lista.append('self.base')
                elif tipo == 0:
                    lista.append('self.datum["'+keys[indice]+'"]')
                indice += 1
            if len(lista) > 1:  # are two or more tables
                if len(lista) > 2:  # more than two tables
                    task = []
                    if (tipos[0] == 0):  # annexed
                        t = 'self.renameTables("' + \
                            keys[0]+'","'+keys[1]+'")'
                        tamanioKeys = len(keys)
                        campo = keys[0]+'_'+keys[1]
                        agrupar = lista[0]+'[1],'+lista[1] + \
                            '[3]["'+keys[0]+'"][1]'
                        for i in range(2, tamanioKeys):  # nest joins
                            campo += '_'+keys[i]
                            agrupar += ',"'+keys[i-1]+'_"+' + \
                                lista[i]+'[3]["'+keys[i-1]+'"][1]'
                            t = 'self.renameTables("JOIN_ESPECIAL","' + \
                                keys[i]+'",['+t+',"'+keys[i-1]+'"])'
                        # ID,NRO_PROD,CONTROLES_NRO_DATITO
                        t = '('+t+').groupBy(['+agrupar+']).count()'
                        idAnexa = str(self.datum[keys[0]][1])
                        fila = 'Row('
                        for i in range(1, tamanioKeys):
                            fila += str(self.datum[keys[i]][3]
                                        [keys[i-1]][1])+'=row['+str(i)+'],'
                        fila += campo+'_count=row['+str(tamanioKeys)+']'
                        task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row.' +
                                    idAnexa+','+fila+').asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append('getFieldValue(row,"'+campo+'_count")')
                    else:
                        t = 'self.renameTables("' + \
                            keys[0]+'","'+keys[1]+'")'
                        tamanioKeys = len(keys)
                        campo = keys[0]+'_'+keys[1]
                        agrupar = '"'+self.idKey+'"'
                        for i in range(2, tamanioKeys):  # nest joins
                            campo += '_'+keys[i]
                            agrupar += ',"'+keys[i-1]+'_"+' + \
                                lista[i]+'[3]["'+keys[i-1]+'"][1]'
                            t = 'self.renameTables("JOIN_ESPECIAL","' + \
                                keys[i]+'",['+t+',"'+keys[i-1]+'"])'
                        # ID,NRO_PROD,CONTROLES_NRO_DATITO
                        t = '('+t+').groupBy(['+agrupar+']).count()'
                        fila = 'Row('
                        for i in range(2, tamanioKeys):
                            fila += str(self.datum[keys[i]][3]
                                        [keys[i-1]][1])+'=row['+str(i-1)+'],'
                        fila += campo+'_count=row['+str(tamanioKeys-1)+'])'
                        task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t +
                                    '.rdd).map(lambda row:(row[idKey],'+fila+'.asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append('getFieldValue(row,"'+campo+'_count")')
                else:  # two tables
                    if (tipos[0] == -1 and tipos[1] == 0):  # la base y tabla anexa
                        task = []
                        idAnexa = str(self.datum[keys[1]][1])
                        t = '('+lista[1]+'[0].groupBy(' + \
                            lista[1]+'[1]).count())'
                        task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row["'+idAnexa +
                                    '"],Row(mother_'+keys[1]+'_count=row[1]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                        task.append(
                            'getFieldValue(row,"mother_'+keys[1]+'_count")')
                    else:
                        if (tipos[0] == 0 and tipos[1] == 0):  # dos tablas anexas
                            task = []
                            idAnexa = str(self.datum[keys[1]][1])
                            s = self.datum[keys[1]][3][keys[0]][1]
                            t = 'self.renameTables("' + \
                                keys[0]+'","'+keys[1]+'")'
                            # ID,NRO_PROD
                            t = '('+t+').groupBy(['+lista[1]+'[1],' + \
                                lista[1]+'[3]["'+keys[0]+'"][1]]).count()'
                            task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row.'+idAnexa+',Row('+s +
                                        '=row[1],mother_'+keys[0]+'_'+keys[1]+'_count=row[2]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                            task.append(
                                'getFieldValue(row,"mother_'+keys[0]+'_'+keys[1]+'_count")')
#                t = {'k':'compleja','data':task}
            else:  # ind["CONTROLES"] annexed table
                if tipos[0] == 0:  # annexed table
                    task = []
                    idAnexa = str(self.datum[keys[0]][1])
                    t = '('+lista[0]+'[0].groupBy('+lista[0]+'[1]).count())'
                    task.append('(((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(('+t+'.rdd).map(lambda row:(row["'+idAnexa +
                                '"],Row(mother_'+keys[0]+'_count=row[1]).asDict())))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append(
                        'getFieldValue(row,"mother_'+keys[0]+'_count")')
#                    t = {'k':'compleja','data':task}
        else:
            if len(keys) > 1:  # ind["OTROS"]["DATITO"] table/s and field
                if (tipos[0] == -1):  # the base
                    task = []
                    task.append('((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join((self.base.rdd).map(lambda row:(row[idKey],Row(mother_' +
                                keys[1]+'=row["'+keys[1]+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"mother_'+keys[1]+'")')
#                     t = {'k':'compleja','data': task}
                elif (tipos[0] == 0):  # annexed table
                    task = []
                    idAnexa = str(self.datum[keys[0]][1])
                    task.append('((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(((self.datum["'+keys[0]+'"][0].rdd)).map(lambda row:(row["' +
                                idAnexa+'"],Row(mother_'+keys[1]+'=row["'+keys[1]+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"mother_'+keys[1]+'")')
#                    t = {'k':'compleja','data':task}
            else:  # just the field
                if tipos[0] == 1:  # is the field of the base
                    task = []
                    task.append('((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join((self.base.rdd).map(lambda row:(row[idKey],Row(mother_' +
                                keys[0]+'=row["'+keys[0]+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append('getFieldValue(row,"mother_'+keys[0]+'")')
#                    t = {'k':'compleja','data':task}
                elif tipos[0] == 2:  # is a field of an annex
                    task = []
                    idAnexa = str(self.datum[keys[0]['tabla']][1])
                    task.append('((self.base.rdd).map(lambda row:(row[motherKey],row.asDict()))).join(((self.datum["'+keys[0]['tabla']+'"][0].rdd)).map(lambda row:(row["' +
                                idAnexa+'"],Row(mother_'+keys[0]['campo']+'=row["'+keys[0]['campo']+'"]).asDict()))).map(lambda x: {**x[1][0],**x[1][1]}).map(lambda x: (Row(**x)))')
                    task.append(
                        'getFieldValue(row,"mother_'+keys[0]['campo']+'")')
        t = {'k': 'compleja', 'data': task}
        return t
########################################################################


class TreeContext():
    def __init__(self, spark, df, columnas, nombreTabla, id, mother, motherEmptyParam, birth_order=None):
        self.datum = {}  # annexed tables
        self.columnas = columnas
        self.nombreBase = nombreTabla
        self.idKey = id
        self.motherKey = mother
        self.motherEP = motherEmptyParam
        self.birthOrderKey = birth_order
        self.spark = spark

        # It replace null with blanks to avoid conversion problems
        self.base = df.na.fill("")

    def collect(self):
        return self.base

    def filter(self, filterFunction):
        tc = FilteredTreeContext(self.spark, self.base, self.columnas, self.nombreBase,
                                 self.idKey, self.motherKey, self.motherEP, self.birthOrderKey)
        tc = tc.stackFunctions(filterFunction)
        tc = tc.setDatum(self.datum)
        tareas = self.parse(filterFunction)
        for t in tareas:
            tc = tc.addTask(t)
        return tc

    def parse(self, funcion):
        ind = Ind(self.base, self.idKey, self.columnas,
                  self.nombreBase, self.datum)

        # is left with the whole func after the lambda ind:
        fuente = inspect.getsource(funcion)
        fuente = fuente[fuente.find(':'):]
        fuente = fuente.strip().split(':')[1][:-1].strip()

        # separate the function by the keywords
        p = fuente.split('__')
        temp_a = [x.split('.count()') for x in p]
        f = []
        f_aux = []
        for temp_b in temp_a:
            for temp_c in temp_b:
                temp_d = (temp_c.split('.value()'))
                for temp_e in temp_d:
                    temp_f = temp_e.replace(
                        'ind.mother', 'motherOfInd.*mother')
                    temp_e = temp_f.replace('ind.', '').split('ind')
                    for temp_g in temp_e:
                        temp_h = temp_g.split('motherOfInd.')
                        for temp_i in temp_h:
                            f_aux.append(temp_i)

        # In the case of calls to functions ind.mother.__func__() the format of the list must be corrected
        ok = False
        for elemento in f_aux:
            if ok:  # only if the previous element is *mother.
                elemento = 'mother_'+elemento  # mother_func()
                ok = False
            if elemento == '*mother.':
                ok = True
            else:
                f.append(elemento)

        # takes the conditions, the filter functions and the preprocessing functions
        condiciones = []
        functions = []
        preprocesamiento = []
        for index in range(0, len(f)):
            value = f[index]
            if index % 2 == 0:
                if value != '':
                    if index != 0 and value[0] == '(':
                        # remove the remaining parentheses
                        value = value[2:]
                condiciones.append(value)
            else:
                if value != '':
                    if value[:7] == '*mother':
                        # I remove the *mother
                        a = ind.processFunction(value[7:], 'M')
                    else:
                        a = ind.processFunction(value)
                    if a["k"] == 'compleja':
                        tam = len(a["data"])
                        for i in range(0, tam-1):
                            preprocesamiento.append(a["data"][i])
                        functions.append(a["data"][tam-1])
                    else:  # comun
                        functions.append(a["data"])

        # eliminate duplicate tasks
        if len(preprocesamiento) > 0:
            noDuplicates = list(dict.fromkeys(preprocesamiento))
            tareas = [p for p in noDuplicates]
        else:
            tareas = []

        # generates the filter expression by binding the functions to the conditions
        i = 0
        aux = ''
        for c in condiciones:
            aux += c
            try:
                aux += functions[i]
            except:
                pass
            i += 1
        # it prepares the tasks to return
        tareas.append('(self.base.rdd).filter(lambda row:'+aux+')')
        return tareas

    def addDatum(self, df, columnas, id, label, foreignKeys):
        self.datum[label] = []
        self.datum[label].append(df.na.fill(""))
        self.datum[label].append(id)
        self.datum[label].append(columnas)
        self.datum[label].append(foreignKeys)
        return self

    def sisters(self, count, perm=False):
        idKey = self.idKey
        motherKey = self.motherKey
        birthOrderKey = self.birthOrderKey
        if perm:  # NON CONSECUTIVE + CONSECUTIVE
            rdd_aux = ((((self.base.rdd).map(lambda row: (row[motherKey], row))).join((self.base.rdd).map(lambda row: (row[motherKey], row)))).map(lambda row: (
                row[1][0][idKey], row[1][1][idKey], row[1][1][birthOrderKey] - row[1][0][birthOrderKey]))).filter(lambda row: row[2] >= 1).map(lambda row: {"ID_IND": row[0], "ID_HERMANA1": row[1]})
        else:  # CONSECUTIVES
            rdd_aux = ((((self.base.rdd).map(lambda row: (row[motherKey], row))).join((self.base.rdd).map(lambda row: (row[motherKey], row)))).map(lambda row: (
                row[1][0][idKey], row[1][1][idKey], row[1][1][birthOrderKey] - row[1][0][birthOrderKey]))).filter(lambda row: row[2] == 1).map(lambda row: {"ID_IND": row[0], "ID_HERMANA1": row[1]})
        copy = rdd_aux.map(lambda row: (row["ID_IND"], row))
        for i in range(1, count-1):
            a = rdd_aux.map(lambda row: (row["ID_HERMANA"+str(i)], row))
            rdd_aux = a.join(copy)
            s = "ID_HERMANA"+str(i+1)
            rdd_aux = rdd_aux.map(lambda row: (
                row[1][0], {s: row[1][1]["ID_HERMANA1"]})).map(lambda x: {**x[0], **x[1]})
        rdd_aux = rdd_aux.map(lambda x: (Row(**x)))
        return rdd_aux.toDF()

    def descendents(self, grade):
        """Prints the individuals that have the degree of descent passed by parameter,
        a degree of descent is the number of descendants that the individual has"""

        idKey = self.idKey
        motherKey = self.motherKey
        motherEP = self.motherEP
        rdd_aux = ((self.base.rdd).filter(lambda row: row[motherKey] != '' or row[motherKey] != motherEP).map(
            lambda row: {"ID_IND": row[motherKey], "ID_DESC1": row[idKey]}))
        copy = rdd_aux.map(lambda row: (row["ID_IND"], row))
        if grade > 2:
            for i in range(1, grade-1):
                a = rdd_aux.map(lambda row: (row["ID_DESC"+str(i)], row))
                rdd_aux = a.join(copy)
                s = "ID_DESC"+str(i+1)
                rdd_aux = rdd_aux.map(lambda row: (row[1][0], {s: row[1][1]["ID_DESC1"]})).map(
                    lambda x: {**x[0], **x[1]})
        rdd_aux = rdd_aux.map(lambda x: (Row(**x)))
        return rdd_aux.toDF()


class FilteredTreeContext(TreeContext):  # WRAPPER
    def __init__(self, spark, df, columnas, nombreTabla, id, mother, motherEmptyParam, birth_order=None):
        super().__init__(spark, df, columnas, nombreTabla,
                         id, mother, motherEmptyParam, birth_order)
        self.functions = []
        self.dag = []

    def stackFunctions(self, func):
        self.functions.append(func)
        return self

    def addTask(self, task):
        self.dag.append(task)
        return self

    def setDatum(self, dicc):
        self.datum = dicc
        return self

    def filter(self, filterFunction):
        self.stackFunctions(filterFunction)
        tareas = self.parse(filterFunction)
        for t in tareas:
            self.addTask(t)
        return self

    def renameTables(self, label1, label2, anexa=None):
        if label1 == 'JOIN_ESPECIAL':
            dataframe = anexa[0]  # tabla de un join anterior
            rddDatum1 = dataframe.rdd
            dataframeID1 = self.datum[anexa[1]][1]
            foreignKeyDF1 = anexa[1]+'_'+self.datum[label2][3][anexa[1]][1]
            rddDatum1 = rddDatum1.map(lambda row: (
                row[dataframeID1]+'-'+str(row[foreignKeyDF1]), row.asDict()))

            # just need to change the name of the other table
            dataframe2 = self.datum[label2][0]
            dataframeID2 = self.datum[label2][1]
            oldColumns = self.datum[label2][2]
            foreignKeyDF2 = self.datum[label2][3][anexa[1]][0]
            newColumns = [label2+'_'+s if s !=
                          dataframeID2 else s for s in oldColumns]
            newColumns = [x if x != label2+'_' +
                          foreignKeyDF2 else foreignKeyDF1 for x in newColumns]
            dataframe2 = reduce(lambda dataframe2, idx: dataframe2.withColumnRenamed(
                oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dataframe2)
            rddDatum2 = (dataframe2.rdd).map(lambda row: (
                row[dataframeID2]+'-'+str(row[foreignKeyDF1]), row.asDict()))

            rddAux = rddDatum1.join(rddDatum2)
        else:
            if label1 == self.nombreBase:  # [IND][CONTROLES]
                # WORK WITH THE BASE
                dataframe1 = self.base
                oldColumns = self.columnas
                newColumns = [self.nombreBase+'_'+s if s !=
                              self.idKey else s for s in oldColumns]
                dataframe1 = reduce(lambda dataframe1, idx: dataframe1.withColumnRenamed(
                    oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dataframe1)
                rddDatos = dataframe1.rdd  # .map(list) #de df a rdd
                idVar = self.idKey
                # DOES NOT PULL THREAD RLOCK ERROR
                rddDatos = rddDatos.map(lambda row: (row[idVar], row.asDict()))

                # WORK WITH THE ANNEXED TABLE
                dataframe = self.datum[label2][0]
                dataframeID = self.datum[label2][1]
                oldColumns = self.datum[label2][2]
                newColumns = [label2+'_'+s if s !=
                              dataframeID else s for s in oldColumns]
                dataframe = reduce(lambda dataframe, idx: dataframe.withColumnRenamed(
                    oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dataframe)
                rddDatum = (dataframe.rdd).map(
                    lambda row: (row[dataframeID], row.asDict()))

                rddAux = rddDatos.join(rddDatum)
    #####################################################################################################################
            else:  # [PRODUCCION][CONTROLES]
                # DF 1
                dataframe1 = self.datum[label1][0]
                dataframeID1 = self.datum[label1][1]
                oldColumns = self.datum[label1][2]
                foreignKeyDF1 = self.datum[label2][3][label1][1]
                newColumns = [label1+'_'+s if s !=
                              dataframeID1 else s for s in oldColumns]
                newColumns = [x if x != label1+'_' +
                              foreignKeyDF1 else foreignKeyDF1 for x in newColumns]
                dataframe1 = reduce(lambda dataframe1, idx: dataframe1.withColumnRenamed(
                    oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dataframe1)
                rddDatum1 = (dataframe1.rdd).map(lambda row: (
                    row[dataframeID1]+'-'+str(row[foreignKeyDF1]), row.asDict()))

                # DF 2
                dataframe = self.datum[label2][0]
                dataframeID2 = self.datum[label2][1]
                oldColumns = self.datum[label2][2]
                foreignKeyDF2 = self.datum[label2][3][label1][0]
                newColumns = [label2+'_'+s if s !=
                              dataframeID2 else s for s in oldColumns]
                newColumns = [x if x != label2+'_' +
                              foreignKeyDF2 else foreignKeyDF1 for x in newColumns]
                dataframe = reduce(lambda dataframe, idx: dataframe.withColumnRenamed(
                    oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dataframe)
                rddDatum2 = (dataframe.rdd).map(lambda row: (
                    row[dataframeID2]+'-'+str(row[foreignKeyDF1]), row.asDict()))

                rddAux = rddDatum1.join(rddDatum2)

        rddAux = rddAux.map(lambda x: {**x[1][0], **x[1][1]})
        rddAux = rddAux.map(lambda x: (Row(**x)))
        df = rddAux.toDF()
        return df

    def collect(self):
        if len(self.dag) > 0:
            noDuplicates = list(dict.fromkeys(self.dag))
            dag = [p for p in noDuplicates]
        else:
            dag = []

        for task in dag:
            resu = eval(task, dict(self=self, motherKey=self.motherKey, motherEP=self.motherEP,
                                   idKey=self.idKey, birthOrderKey=self.birthOrderKey, datetime=dd, Row=Row,
                                   motherExists=motherExists, childrenOrder=childrenOrder, childrenCount=childrenCount,
                                   getFieldValue=getFieldValue, nextSister=nextSister, previousSister=previousSister,
                                   sistersCount=sistersCount, granddaughterCount=granddaughterCount,
                                   mother_motherExists=mother_motherExists, mother_childrenOrder=mother_childrenOrder,
                                   mother_childrenCount=mother_childrenCount, mother_nextSister=mother_nextSister,
                                   mother_previousSister=mother_previousSister, mother_sistersCount=mother_sistersCount,
                                   mother_granddaughterCount=mother_granddaughterCount))
            try:
                df = resu.toDF()
                self.base = df  # I place the new filtered table inside the tc
                self.columnas = df.schema.names
            except:
                return None
        return df


class Utils():

    def unionDF(df1, df2):
        df = (df1.rdd).union(df2.rdd)
        df = df.distinct().toDF()
        return df

    def joinDF(df1, nombreTabla1, columnas1, key1, df2, nombreTabla2, columnas2, key2):
        newColumns1 = [nombreTabla1+'_'+s if s !=
                       key1 else s for s in columnas1]
        df1 = reduce(lambda df1, idx: df1.withColumnRenamed(
            columnas1[idx], newColumns1[idx]), range(len(columnas1)), df1)
        rddDatos1 = df1.rdd.map(lambda row: (row[key1], row.asDict()))

        newColumns2 = [nombreTabla2+'_'+s if s !=
                       key2 else s for s in columnas2]
        df2 = reduce(lambda df2, idx: df2.withColumnRenamed(
            columnas2[idx], newColumns2[idx]), range(len(columnas2)), df2)
        rddDatos2 = df2.rdd.map(lambda row: (row[key2], row.asDict()))

        rddAux = rddDatos1.join(rddDatos2)
        rddAux = rddAux.map(lambda x: {**x[1][0], **x[1][1]})
        rddAux = rddAux.map(lambda x: (Row(**x)))
        df = rddAux.toDF()
        return df
