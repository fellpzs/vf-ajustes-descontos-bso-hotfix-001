#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import boto3
import os
from process_query import execute_query, execute_query_in_db, execute_update_in_container, get_container, get_properties
from json import JSONEncoder
from decimal import Decimal

QUERY = open('query-reprocessador.sql', 'r').read()

DELETE_DETALHE_DESCONTOS_FIDELIDADE_CUPOM = "DELETE FROM item_venda_desconto WHERE trnseq = '{}' and cxanum = '{}' and trndat = '{}' and lojcod = {} and itdori = '3';"
INSERT_DESCONTO_FIDELIDADE = "INSERT INTO public.item_venda_desconto (itv_id, trnseq, cxanum, trndat, lojcod, itdcupseq, itdvlr, itdori, mdecod, itdatv) VALUES({}, '{}', '{}', '{}', {}, '{}', {}, '3', 1, true);"
DELETE_DESCONTO_MOTOR = "DELETE FROM ITEM_VENDA_DESCONTO WHERE ITV_ID = {} AND ITDORI = '1' AND ITDATV is true;"
INSERT_DESCONTO_MOTOR = "INSERT INTO public.item_venda_desconto (itv_id, trnseq, cxanum, trndat, lojcod, itdcupseq, itdvlr, itdori, mdecod, itdidprm, itddesprm, itdatv) VALUES({}, '{}', '{}', '{}', {}, '{}', {}, '1', 0, '{}', E'{}', true);"
DELETE_DESCONTO_MANUAL = "DELETE FROM ITEM_VENDA_DESCONTO WHERE ITV_ID = {} AND ITDORI in ('0', '2') AND ITDATV is true;"
INSERT_DESCONTO_MANUAL = "INSERT INTO public.item_venda_desconto (itv_id, trnseq, cxanum, trndat, lojcod, itdcupseq, itdvlr, itdori, mdecod, itdidprm, itddesprm, itdatv) VALUES({}, '{}', '{}', '{}', {}, '{}', {}, '2', 0, null, null, true);" # Verificar qual motivo desconto informar
SELECT_DESCONTOS_MOTOR_CUPOM = "SELECT * FROM ITEM_VENDA_DESCONTO WHERE ITV_ID in (select id from item_venda iv where iv.lojcod = {} and iv.trnseq = '{}' and iv.trndat = '{}' and iv.cxanum = '{}') AND ITDORI = '1' AND ITDATV is true order by itdcupseq;"
UPDATE_ITEM_VENDA_DESCONTO_MOTOR = "UPDATE ITEM_VENDA_DESCONTO SET ITDVLR = {} WHERE ID = {};"
DELETE_DESCONTO_NAO_CLASSIFICADO = "DELETE FROM ITEM_VENDA_DESCONTO WHERE ITV_ID = {} AND itdidprm = '{}' AND ITDORI = '1' AND ITDATV is true;"
INSERT_DESCONTO_NAO_CLASSIFICADO = "INSERT INTO public.item_venda_desconto (itv_id, trnseq, cxanum, trndat, lojcod, itdcupseq, itdvlr, itdori, mdecod, itdidprm, itddesprm, itdatv) VALUES({}, '{}', '{}', '{}', {}, '{}', {}, '1', 0, '{}', E'{}', true);"
INSERT_UPDATE_STATUS_CUPOM = "INSERT INTO public.reprocessamento_descontos_bso (identificador, status, exportado) VALUES('{}', '{}', true) ON CONFLICT (identificador) DO UPDATE SET status = '{}';"
SELECT_CUPONS_PROCESSADOS = "SELECT * FROM public.reprocessamento_descontos_bso WHERE STATUS = 'SUCESSO';"

INSERT_REENVIO_CUPOM = "insert into fila_reenvio_mobile (identificador, filtro) VALUES('VENDA', E'{{\"loja\": {0}, \"sequencial\": \"{1}\", \"caixa\": \"{2}\", \"dataInicial\": \"{3}\", \"dataFinal\": \"{3}\"}}');"

ID_PROMO_AJUSTE = "46d76216-d71c-49f5-af02-e674713dac0d"  # Link: https://slackcm.slack.com/archives/DMUP1N6P4/p1674158456009059
DESC_PROMO_AJUSTE = "Reprocessamento Descontos CM"

MOTOR_DB_HOST = "localhost"  # definir variaveis
MOTOR_DB_PORT = "5432"       # definir variaveis
MOTOR_DB_DATABASE = "reprocessamento" # definir variaveis
MOTOR_DB_USER = "postgres"   # definir variaveis
MOTOR_DB_PASS = "123"        # definir variaveis

GENERATE_IN_TASKS = False
EXECUTE_QUERIES_IMMEDIATELY = True
LIMIT_QUERIES_TO_FILE = 500
REENVIO_BUCKET_NAME = ''     #definir variavel
generatedFiles = 0

globalResult = []

totalDescontoCupom = Decimal('0.00')
totalDescontoFidelidadeCupom = Decimal('0.00')
totalDescontoMotorCupom = Decimal('0.00')
totalDescontoManualCupom = Decimal('0.00')
totalDescontoNaoClassificadoCupom = Decimal('0.00')

environment = "local"

class BatchUpdateByContainer:
    queries = []
    def __init__(self, name):
        self.name = name
        self.queries = []

class BatchUpdateByContainerEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__

def createResultJson(obj, generateInTasks):
    
    if(generateInTasks):
        queries = convert_to_insert_on_tasks(obj.queries)
        obj.queries = queries


def convert_to_insert_on_tasks(queries):
    BATCH_SIZE = 500
    INSERT_INTO_TASK_PENDENTE = "INSERT INTO task_pendente (id, tipo, comando) VALUES (nextval('sq_task_pendente'), 'PADRAO', E'{}');"
    queriesTask = []
    for i in range(0, len(queries), BATCH_SIZE):
        queriesTask.append(INSERT_INTO_TASK_PENDENTE.format("".join(queries[i:i+BATCH_SIZE]).replace(r"\'", r"''").replace("\'", r"\'")))

    return queriesTask

def ajustesDescontosFidelidade(cupomData, ajusteCupom):
    global globalResult
    global totalDescontoCupom
    global totalDescontoFidelidadeCupom
    
    total_desconto_fidelidade_item_rat = Decimal('0.00')
    total_det_desconto_fideli = Decimal('0.00')
    total_desconto_fidelidade = Decimal('0.00')
    total_desconto_total_item_rat = Decimal('0.00')
    for row in cupomData:
        total_desconto_fidelidade_item_rat += row["desconto_fidelidade_item_rat"]
        total_desconto_total_item_rat += round(row["fator_rateio"] * row["total_desconto"], 2)
        total_det_desconto_fideli += row["det_desconto_fideli"]
        total_desconto_fidelidade = row["total_desconto_fidelidade"]
        totalDescontoCupom = row["total_desconto"]
    
    if (total_desconto_fidelidade > totalDescontoCupom):
        ajusteCupom.queries.append(DELETE_DETALHE_DESCONTOS_FIDELIDADE_CUPOM.format(row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"]))
        total_desconto_total_item_rat = sum(round(row["fator_rateio"] * row["total_desconto"], 2) for row in cupomData)
        diferenca_rateio = totalDescontoCupom - total_desconto_total_item_rat
        for row in cupomData:
            desconto_total_item_rat = round(row["fator_rateio"] * row["total_desconto"], 2)
            if(row["max_item"] == int(row["seq_item"]) and diferenca_rateio != Decimal("0.00")):    
                totalDescontoFidelidadeCupom += (desconto_total_item_rat + diferenca_rateio)
                ajusteCupom.queries.append(INSERT_DESCONTO_FIDELIDADE.format(
                    row["id_item"], row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"], row["seq_item"], (desconto_total_item_rat + diferenca_rateio)))
            else:
                totalDescontoFidelidadeCupom += desconto_total_item_rat
                ajusteCupom.queries.append(INSERT_DESCONTO_FIDELIDADE.format(
                    row["id_item"], row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"], row["seq_item"], desconto_total_item_rat))
            
        if(len(ajusteCupom.queries) > 0):
            globalResult.append(ajusteCupom)
              
    elif(total_desconto_fidelidade != total_det_desconto_fideli):
        ajusteCupom.queries.append(DELETE_DETALHE_DESCONTOS_FIDELIDADE_CUPOM.format(row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"]))
        diferenca_rateio = total_desconto_fidelidade - total_desconto_fidelidade_item_rat
        for row in cupomData:
            if(row["tipo_pdv"] == 'MOBSHOP'):
                continue
                
            if(row["max_item"] == int(row["seq_item"]) and diferenca_rateio != Decimal("0.00")):    
                totalDescontoFidelidadeCupom += (row["desconto_fidelidade_item_rat"] + diferenca_rateio)
                ajusteCupom.queries.append(INSERT_DESCONTO_FIDELIDADE.format(
                    row["id_item"], row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"], row["seq_item"], (row["desconto_fidelidade_item_rat"] + diferenca_rateio)))
            else:
                totalDescontoFidelidadeCupom += row["desconto_fidelidade_item_rat"]
                ajusteCupom.queries.append(INSERT_DESCONTO_FIDELIDADE.format(
                    row["id_item"], row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"], row["seq_item"], row["desconto_fidelidade_item_rat"]))
            
        if(len(ajusteCupom.queries) > 0):
            globalResult.append(ajusteCupom)    
    else:
        totalDescontoFidelidadeCupom = total_desconto_fidelidade
        
def getCupomParaAjuste(listaCupom, nomeLink):
    try:
        return listaCupom[0]
    except IndexError:
        return BatchUpdateByContainer(nomeLink)
    
def getFirstOrElse(lista, orElse):
    try:
        return lista[0]
    except IndexError:
        return orElse

def removeDescontoMotorItemExistenteEAdicionaNovos(row, descontosMotorItemList, ajusteCupom):
    global totalDescontoMotorCupom
    
    ajusteCupom.queries.append(DELETE_DESCONTO_MOTOR.format(row["id_item"]))
    for descontoItem in descontosMotorItemList:
        totalDescontoMotorCupom += descontoItem["desconto"]
        ajusteCupom.queries.append(INSERT_DESCONTO_MOTOR.format(row["id_item"], row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"], row["seq_item"], descontoItem["desconto"], descontoItem["idPromo"], descontoItem["descPromo"].replace("\'", r"\'")))
    
def verificarSeDescontosMotorExcedeMaximo(maxDescontoMotor, vendaJSON):
    
    totalDescontosMotorItem = sum([round(Decimal(str(desconto["vlrdesconto"])), 2) for desconto in vendaJSON])
    return maxDescontoMotor < totalDescontosMotorItem

def verificarSeDescontosMotorExistenteExcedeMaximo(maxDescontoMotor, cupomData):
    totalDescontosMotorItem = sum(round(row["det_desconto_motor"], 2) for row in cupomData )
    return maxDescontoMotor < totalDescontosMotorItem
    
def recalcularDescontosMotor(row, vendaJSON, maxDescontoMotor):
    totalDescontosMotorVendaJSON = Decimal('0.00')
    promocoesList = set(())
    for descontosItens in vendaJSON:
        totalDescontosMotorVendaJSON += round(Decimal(str(descontosItens["vlrdesconto"])), 2)
        promocoesList.add(descontosItens["idprm"])
    
    novosDescontosCalculados = Decimal('0.00')
    qtdPromo = len(promocoesList)
    count1 = 0
    for promo in promocoesList:
        itensPromo = list(filter(lambda x: x["idprm"] == promo, vendaJSON))
        count1 += 1
        for descontosItens in itensPromo:
                roundCalculatedDiscount = round((round(Decimal(str(descontosItens["vlrdesconto"])), 2) * maxDescontoMotor) / totalDescontosMotorVendaJSON, 2)
                novosDescontosCalculados += roundCalculatedDiscount
                descontosItens["vlrdesconto"] = Decimal(str(roundCalculatedDiscount))
                
                if(count1 == qtdPromo and descontosItens == itensPromo[-1]):
                    if (novosDescontosCalculados != maxDescontoMotor):
                        diferencaAjustar = maxDescontoMotor - novosDescontosCalculados
                        if(diferencaAjustar < 0.00 ):
                            vlrdesconto = Decimal(str(itensPromo[-1]["vlrdesconto"]))
                            if(vlrdesconto + diferencaAjustar < 0.00):
                                diferencaAjustar += vlrdesconto 
                                itensPromo[-1]["vlrdesconto"]  = Decimal(0.00)
                                try:
                                    vlrdesconto = Decimal(str(itensPromo[-2]["vlrdesconto"]))
                                    vlrdesconto += diferencaAjustar
                                    if (vlrdesconto >= 0):
                                        itensPromo[-2]["vlrdesconto"] = vlrdesconto
                                    else:
                                        itensPromo[-2]["vlrdesconto"]  = Decimal(0.00)                                        
                                        maxDiscountItem = max(vendaJSON, key=lambda x:Decimal(str(x["vlrdesconto"])))
                                        maxDiscountItem["vlrdesconto"] += vlrdesconto                                         
                                except:
                                    maxDiscountItem = max(vendaJSON, key=lambda x:Decimal(str(x["vlrdesconto"])))
                                    maxDiscountItem["vlrdesconto"] += diferencaAjustar
                            else:
                                itensPromo[-1]["vlrdesconto"] = round(Decimal(str(itensPromo[-1]["vlrdesconto"])), 2) + diferencaAjustar        
                        else:
                            itensPromo[-1]["vlrdesconto"] = round(Decimal(str(itensPromo[-1]["vlrdesconto"])), 2) + diferencaAjustar        
                                                        

def recalcularDescontosExistentesMotor(cupomData, maxDescontoMotor, ajusteCupom):
    global environment
    row = getFirstOrElse(cupomData, None)
    totalDescontosMotor = sum(round(row["det_desconto_motor"], 2) for row in cupomData )
    descontosMotorCupom = execute_query(environment, SELECT_DESCONTOS_MOTOR_CUPOM.format(row["lojcod"], row["trnseq"], str(row["trndat"]), row["cxanum"]), ajusteCupom.name)
    updatesDescontosList = []
    sumDescontos = Decimal('0.00')
    for descontoMot in descontosMotorCupom[0].result:
        valorAtualizar = round(((descontoMot["itdvlr"] * totalDescontosMotor) / maxDescontoMotor), 2)
        updatesDescontos = {
            "id": descontoMot["id"],
            "valor": valorAtualizar
        }
        sumDescontos += valorAtualizar
        updatesDescontosList.append(updatesDescontos)
    
    if(sumDescontos != 0.00 and sumDescontos > maxDescontoMotor):
        diferenca = maxDescontoMotor - sumDescontos
        count = -1
        while(diferenca != 0.00):
            descontoAjuste = updatesDescontosList[count]
            if(descontoAjuste["valor"] + diferenca < 0.00):
                diferenca += descontoAjuste["valor"]
                descontoAjuste["valor"] = Decimal('0.00')
                count -= 1
            else:                
                descontoAjuste["valor"] += diferenca 
                diferenca = Decimal('0.00')                
    
    for descontoMot in updatesDescontosList:    
        ajusteCupom.queries.append(UPDATE_ITEM_VENDA_DESCONTO_MOTOR.format(descontoMot["valor"], descontoMot["id"]))
    
           
def verificaSeDescontosMotorIgualAoExistente(vendaJSON, total_det_desconto_motor):
    totalDescontosMotorCupom = Decimal('0.00')
    totalDescontosMotorCupom = sum([round(Decimal(str(desconto["vlrdesconto"])), 2) for desconto in vendaJSON])
    
    return total_det_desconto_motor == totalDescontosMotorCupom
    
def consultarDadosBaseMotor(row):
    try:
        QUERY_CONSULTA_BASE_MOTOR = '''
                select * 
                from itens_descontos_motor 
                where lojcod = {} 
                and trnseq = {} 
                and cxanum = {} 
                and trndat = '{}' 
                and vlrdesconto is not null;
            '''
        query = QUERY_CONSULTA_BASE_MOTOR.format(int(row["lojcod"]), int(row["trnseq"]), int(row["cxanum"]), row["trndat"])
        dbConfigAndQuery = {
            "host": MOTOR_DB_HOST,
            "port": MOTOR_DB_PORT,
            "database": MOTOR_DB_DATABASE,
            "user": MOTOR_DB_USER,
            "password": MOTOR_DB_PASS,
            "query": query
        }
        vendaJSON = execute_query_in_db(dbConfigAndQuery)
        if (not vendaJSON):
            query = QUERY_CONSULTA_BASE_MOTOR.format(int(row["lojcod"]), int(row["numero_nota"]), int(row["cxanum"]), row["trndat"])
            dbConfigAndQuery = {
                "host": MOTOR_DB_HOST,
                "port": MOTOR_DB_PORT,
                "database": MOTOR_DB_DATABASE,
                "user": MOTOR_DB_USER,
                "password": MOTOR_DB_PASS,
                "query": query
            }
            vendaJSON = execute_query_in_db(dbConfigAndQuery)
             
        return vendaJSON
    except Exception as e:
        print(('Erro ao consultar BD com descontos do Motor: \n{}').format(e))
        vendaJSON = None
                
def ajustesDescontosMotor(cupomData, ajusteCupom):
    global globalResult
    global totalDescontoMotorCupom
    global totalDescontoNaoClassificadoCupom
            
    vendaJSON = []
    maxDescontoMotor = Decimal('0.00')
    total_det_desconto_motor = Decimal('0.00')    
    total_det_desconto_motor = sum([row["det_desconto_motor"] for row in cupomData])
        
    row = getFirstOrElse(cupomData, None)
    
    if (row):
        maxDescontoMotor = row["total_desconto"] - row["total_desconto_fidelidade"]
        if maxDescontoMotor == 0.00:
            for itemCupom in cupomData:
                ajusteCupom.queries.append(DELETE_DESCONTO_MOTOR.format(itemCupom["id_item"]))
            return
        if(not vendaJSON or not(int(vendaJSON[0]["lojcod"]) == int(row["lojcod"]) and int(vendaJSON[0]["trnseq"]) == int(row["trnseq"]) and int(vendaJSON[0]["cxanum"]) == int(row["cxanum"]))):
            vendaJSON = consultarDadosBaseMotor(row)
                
        if (vendaJSON):
            
            if (verificaSeDescontosMotorIgualAoExistente(vendaJSON, total_det_desconto_motor) and not verificarSeDescontosMotorExcedeMaximo(maxDescontoMotor, vendaJSON)):
                totalDescontoMotorCupom = total_det_desconto_motor
                return               
            
            if(verificarSeDescontosMotorExcedeMaximo(maxDescontoMotor, vendaJSON)):              
                recalcularDescontosMotor(row, vendaJSON, maxDescontoMotor)
                
            totalMotorRecalculado = sum([round(Decimal(str(desconto["vlrdesconto"])), 2) for desconto in vendaJSON])
            
            if (total_det_desconto_motor > totalMotorRecalculado):
                totalDescontoNaoClassificadoCupom += (total_det_desconto_motor - totalMotorRecalculado)
            
            for itemCupom in cupomData:
                descontosMotorItemList = []                                        
                for descontoMotorItem in list(filter(lambda x: x["seqitem"] == int(itemCupom["seq_item"]), vendaJSON)):
                    descontosMotorItem = {
                        "idPromo": descontoMotorItem["idprm"],
                        "descPromo": descontoMotorItem["dscprm"],
                        "desconto": round(Decimal(str(descontoMotorItem["vlrdesconto"])), 2)
                    }
                    descontosMotorItemList.append(descontosMotorItem)
            
                removeDescontoMotorItemExistenteEAdicionaNovos(itemCupom, descontosMotorItemList, ajusteCupom)                
        
        else:
            if(verificarSeDescontosMotorExistenteExcedeMaximo(maxDescontoMotor, cupomData)):      
                recalcularDescontosExistentesMotor(cupomData, maxDescontoMotor, ajusteCupom)
                
            totalDescontoMotorCupom = total_det_desconto_motor
            return

def ajustesDescontosManual(cupomData, ajusteCupom):  
    
    global totalDescontoManualCupom
    
    valorMaximoDescontoManual = totalDescontoCupom - (totalDescontoFidelidadeCupom + totalDescontoMotorCupom)
    valorAjusteDescontoManual = Decimal('0.00')
    valorAjusteAplicado = Decimal('0.00')
    
    row = getFirstOrElse(cupomData, None)
            
    desconto_manual_sub_cupom = row["total_desconto_mega"] - row["total_desconto_fidelidade"]
    total_desconto_manual_item_atual =  sum([itemCupom["det_desconto_man_item"] for itemCupom in cupomData])
    total_desconto_manual_sub_atual =  sum([itemCupom["det_desconto_man_sub"] for itemCupom in cupomData])
    total_desconto_manual_atual = total_desconto_manual_item_atual + total_desconto_manual_sub_atual    
    if (row["tipo_pdv"] == 'MOBSHOP'):
        totalDescontoManualCupom += total_desconto_manual_atual
        return
    
    if (desconto_manual_sub_cupom > 0 and desconto_manual_sub_cupom > total_desconto_manual_sub_atual):
        valorAjusteDescontoManual = desconto_manual_sub_cupom + total_desconto_manual_item_atual
    else:
        valorAjusteDescontoManual = total_desconto_manual_atual
        
    if (valorMaximoDescontoManual <= 0.00):
        for itemCupom in cupomData:
            ajusteCupom.queries.append(DELETE_DESCONTO_MANUAL.format(itemCupom["id_item"]))
        return
    
    if (valorAjusteDescontoManual > valorMaximoDescontoManual):
        valorAjusteDescontoManual = valorMaximoDescontoManual
     
    valoresAjustarList =[]        
    for itemCupom in cupomData:
        ajusteCupom.queries.append(DELETE_DESCONTO_MANUAL.format(itemCupom["id_item"]))
        valorDescontoManRat = round(Decimal(str(itemCupom["fator_rateio"] * valorAjusteDescontoManual)), 2)
        valorAjusteAplicado += valorDescontoManRat
        if (itemCupom["max_item"] == int(itemCupom["seq_item"]) and valorAjusteAplicado != valorAjusteDescontoManual):
            diferencaRateio = valorAjusteDescontoManual - valorAjusteAplicado
            valorDescontoManRat = valorDescontoManRat + diferencaRateio                    
        if (valorDescontoManRat > 0.00):
            dadosAjuste = {
                "seq_item": itemCupom["seq_item"],
                "id_item": itemCupom["id_item"],
                "trnseq": itemCupom["trnseq"],
                "trndat": itemCupom["trndat"],
                "lojcod": itemCupom["lojcod"],
                "cxanum": itemCupom["cxanum"],
                "valor": valorDescontoManRat
                }
            valoresAjustarList.append(dadosAjuste)
        elif (valorDescontoManRat < 0.00):
            count = -1
            while(valorDescontoManRat < 0.00):
                try:
                    dadosAjuste = valoresAjustarList[count]
                    if dadosAjuste["valor"] < (valorDescontoManRat * -1):
                        valorDescontoManRat += dadosAjuste["valor"]
                        dadosAjuste["valor"] = Decimal('0.00')
                    else:
                        dadosAjuste["valor"] += valorDescontoManRat
                        valorDescontoManRat = Decimal('0.00')
                except:
                    valorDescontoManRat = Decimal('0.00')
                    print("Não é possível ajustar valor desconto manual")
                count -= 1
            
    for valorAjustar in valoresAjustarList:
        totalDescontoManualCupom += valorAjustar["valor"]
        ajusteCupom.queries.append(INSERT_DESCONTO_MANUAL.format(valorAjustar["id_item"], valorAjustar["trnseq"], valorAjustar["cxanum"], valorAjustar["trndat"], valorAjustar["lojcod"], valorAjustar["seq_item"], valorAjustar["valor"]))
    
    
def ajustesDescontosNaoClassificados(cupomData, ajusteCupom):
    
    global totalDescontoNaoClassificadoCupom
    
    valorAjusteDescontoNaoClassificado = totalDescontoCupom - (totalDescontoFidelidadeCupom + totalDescontoMotorCupom + totalDescontoManualCupom)
    valorAjusteAplicado = Decimal('0.00')
    if (valorAjusteDescontoNaoClassificado > 0.00):
        valoresAjustarList =[]        
        for itemCupom in cupomData:
            ajusteCupom.queries.append(DELETE_DESCONTO_NAO_CLASSIFICADO.format(itemCupom["id_item"], ID_PROMO_AJUSTE))
            valorDescontoNaoClassificadoRat = round(Decimal(str(itemCupom["fator_rateio"] * valorAjusteDescontoNaoClassificado)), 2)
            temAjusteBrinde = False
            if(itemCupom["total_desconto_item"] > 0.00 and valorDescontoNaoClassificadoRat == 0.00):
                matches = [match for match in ajusteCupom.queries if "VALUES({}".format(itemCupom["id_item"]) in match]
                if not matches:
                    temAjusteBrinde = True
                    valorDescontoNaoClassificadoRat = Decimal('0.01')
            valorAjusteAplicado += valorDescontoNaoClassificadoRat
            if (itemCupom["max_item"] == int(itemCupom["seq_item"]) and valorAjusteAplicado != valorAjusteDescontoNaoClassificado):
                diferencaRateio = valorAjusteDescontoNaoClassificado - valorAjusteAplicado
                valorDescontoNaoClassificadoRat = valorDescontoNaoClassificadoRat + diferencaRateio                    
            if (valorDescontoNaoClassificadoRat > 0.00 or (valorDescontoNaoClassificadoRat == 0.00 and temAjusteBrinde)):
                dadosAjuste = {
                    "seq_item": itemCupom["seq_item"],
                    "id_item": itemCupom["id_item"],
                    "trnseq": itemCupom["trnseq"],
                    "trndat": itemCupom["trndat"],
                    "lojcod": itemCupom["lojcod"],
                    "cxanum": itemCupom["cxanum"],
                    "valor": valorDescontoNaoClassificadoRat
                    }
                valoresAjustarList.append(dadosAjuste)
            elif (valorDescontoNaoClassificadoRat < 0.00):
                count = -1
                while(valorDescontoNaoClassificadoRat < 0.00):
                    try:
                        dadosAjuste = valoresAjustarList[count]
                        if dadosAjuste["valor"] < (valorDescontoNaoClassificadoRat * -1):
                            valorDescontoNaoClassificadoRat += dadosAjuste["valor"]
                            dadosAjuste["valor"] = Decimal('0.00')
                        else:
                            dadosAjuste["valor"] += valorDescontoNaoClassificadoRat
                            valorDescontoNaoClassificadoRat = Decimal('0.00')
                    except:
                        valorDescontoNaoClassificadoRat = Decimal('0.00')
                        print("Não é possível ajustar valor desconto não classificado")
                    count -= 1
                
        for valorAjustar in valoresAjustarList:
            ajusteCupom.queries.append(INSERT_DESCONTO_NAO_CLASSIFICADO.format(valorAjustar["id_item"], valorAjustar["trnseq"], valorAjustar["cxanum"], valorAjustar["trndat"], valorAjustar["lojcod"], valorAjustar["seq_item"], valorAjustar["valor"], ID_PROMO_AJUSTE, DESC_PROMO_AJUSTE.replace("\'", r"\'")))
            
    elif (valorAjusteDescontoNaoClassificado <= 0.00):
        for itemCupom in cupomData:
            ajusteCupom.queries.append(DELETE_DESCONTO_NAO_CLASSIFICADO.format(itemCupom["id_item"], ID_PROMO_AJUSTE))

def verificarSeNecessitaAjusteDesconto(cupomData):
    global totalDescontoCupom            
    total_desconto_detalhe = Decimal('0.00')
    for itemCupom in cupomData:
        total_desconto_detalhe += itemCupom["det_total_descontos"]
        totalDescontoCupom = itemCupom["total_desconto"]
    
    return total_desconto_detalhe != totalDescontoCupom
            
def gravarArquivo(fileName, obj):
    updatesOut = json.dumps(obj, cls=BatchUpdateByContainerEncoder)
    with open(fileName, 'w') as outfile:        
        outfile.write(updatesOut)               
    
    
def salvaProcessamentoCupom(identificador, status) :
    INSERT_UPDATE_STATUS_CUPOM(identificador, status)

def main(): 
    global globalResult
    global totalDescontoCupom
    global totalDescontoFidelidadeCupom
    global totalDescontoMotorCupom
    global totalDescontoManualCupom
    global totalDescontoNaoClassificadoCupom
    
    global generatedFiles
    
    globalResultsDeReenvio = []
    
    # IMPLEMENTAR PARADA DO LINK
    
    instanceName = os.getenv("instanceName") 
    dataInicio = os.getenv("dataInicio", "2022-01-01")
    dataFim = os.getenv("dataFim", "2023-03-31")
    cuponsReprocessar = execute_query(QUERY.format(dataInicio, dataFim), instanceName)    
    
    ajusteCupom = BatchUpdateByContainer(instanceName)
    cupomPK = ""
    
    nomeArqReenvio = '{}.csv'.format(cuponsReprocessar.name)
    
    container_data = get_container(instanceName)
    props = get_properties(container_data['id'])
    
    cupomData = []
    for row in cuponsReprocessar.result:
        salvaProcessamentoCupom(row['id_transacao'], 'PROCESSANDO')
        if(cupomPK != "{}_{}_{}_{}".format(row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"]) and len(cupomData) > 0):
            
            totalDescontoCupom = Decimal('0.00')
            totalDescontoFidelidadeCupom = Decimal('0.00')
            totalDescontoMotorCupom = Decimal('0.00')
            totalDescontoManualCupom = Decimal('0.00')
            totalDescontoNaoClassificadoCupom = Decimal('0.00')
            
            if(verificarSeNecessitaAjusteDesconto(cupomData)):
                try:
                    tamanhoAntes = len(ajusteCupom.queries)
                    ajustesDescontosFidelidade(cupomData, ajusteCupom)    
                    ajustesDescontosMotor(cupomData, ajusteCupom)
                    ajustesDescontosManual(cupomData, ajusteCupom)
                    ajustesDescontosNaoClassificados(cupomData, ajusteCupom)
                    tamanhoDepois = len(ajusteCupom.queries)
                    if (tamanhoAntes != tamanhoDepois):
                        salvaProcessamentoCupom(row['id_transacao'], 'SUCESSO')
                        itemCupom = getFirstOrElse(cupomData, None)
                    
                    if(len(ajusteCupom.queries) >= LIMIT_QUERIES_TO_FILE):
                        if EXECUTE_QUERIES_IMMEDIATELY :                         
                            execute_update_in_container(ajusteCupom.queries, ajusteCupom.name, container_data, props)
                        
                        ajusteCupom = BatchUpdateByContainer(cuponsReprocessar.name)
                    
                    print(f"Cupom {cupomPK} processado.")
                except Exception as err:
                    salvaProcessamentoCupom(row['id_transacao'], 'ERRO')
                    print(f"Cupom: {cupomPK} com erro inesperado: {err}, {type(err)}")
            
    cupomData = []
    
    cupomPK = "{}_{}_{}_{}".format(row["trnseq"], row["cxanum"], row["trndat"], row["lojcod"])
    itemData = {**row, "cupomPK": cupomPK} 
    cupomData.append(itemData)
        
    if(len(cupomData) > 0):
        totalDescontoCupom = Decimal('0.00')
        totalDescontoFidelidadeCupom = Decimal('0.00')
        totalDescontoMotorCupom = Decimal('0.00')
        totalDescontoManualCupom = Decimal('0.00')
        totalDescontoNaoClassificadoCupom = Decimal('0.00')
        
        if(verificarSeNecessitaAjusteDesconto(cupomData)):
            try:
                tamanhoAntes = len(ajusteCupom.queries)
                ajustesDescontosFidelidade(cupomData, ajusteCupom)    
                ajustesDescontosMotor(cupomData, ajusteCupom)
                ajustesDescontosManual(cupomData, ajusteCupom)
                ajustesDescontosNaoClassificados(cupomData, ajusteCupom)
                tamanhoDepois = len(ajusteCupom.queries)
                
                if (tamanhoAntes != tamanhoDepois):
                    itemCupom = getFirstOrElse(cupomData, None)

                print(f"Cupom {cupomPK} processado.")
            except Exception as err:
                    salvaProcessamentoCupom(row['id_transacao'], 'ERRO')
                    print(f"Cupom: {cupomPK} com erro inesperado: {err}, {type(err)}")

        if (len(ajusteCupom.queries) > 0):
            if EXECUTE_QUERIES_IMMEDIATELY :                
                execute_update_in_container(ajusteCupom.queries, ajusteCupom.name, container_data, props)
                            
        if(EXECUTE_QUERIES_IMMEDIATELY and not GENERATE_IN_TASKS) :
            cuponsProcessados = execute_query(environment, SELECT_CUPONS_PROCESSADOS)
            file1 = open(nomeArqReenvio, "a")
            for cupomProcessado in cuponsProcessados.result:
                file1.write(f"{cupomProcessado['identificador']}\n")
            file1.close()

            s3 = boto3.resource('s3')                
            data = open(nomeArqReenvio, 'rb')
            s3.Bucket(REENVIO_BUCKET_NAME).put_object(Key=nomeArqReenvio, Body=data)
                        
    
    # IMPLEMENTAR START DO LINK    
    print("Fim do processamento - OK")
    

if __name__ == '__main__':
        
    main()