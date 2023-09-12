create table if not exists reprocessamento_descontos_bso (
    identificador bigint NOT NULL primary key,
    status varchar(20) NOT NULL,
    exportado boolean NOT NULL default false
);
select t.identificador id_transacao, t.lojcod, pl.loj_codigo_dominio tipo_loja, t.trnseq, t.trndat, t.cxanum, c.cxa_frente_loja tipo_pdv,
coalesce(pv.pva_origem, 'NÃO') origem_prevenda,
coalesce(substring(nullif(t.TRNCHVCFE, ''),32,6),substring(nullif(t.TRNNFCECHVNFE,''),26,9),t.TRNSEQEQP::text) numero_nota, 
count(iv.*) over (partition by t.lojcod, t.trnseq, t.trndat, t.cxanum)  qtd_itens_cupom,
t.trnvlr + t.trndcn - t.trnacr total_valor_bruto, t.trnvlr total_valor_liquido, t.trnacr total_acrescimo, t.trndcn total_desconto, coalesce(t.trndcnfid, 0.00) total_desconto_fidelidade, coalesce(t.trndcnmgc, 0.0) total_desconto_mega,
iv.id id_item, coalesce(nullif(trim(iv.itvseq), ''), iv.id::text) seq_item, max(cast(coalesce(nullif(trim(iv.itvseq), ''), iv.id::text) as integer)) over (partition by t.lojcod, t.trnseq, t.trndat, t.cxanum) max_item, iv.procod, iv.itvqtdvda qtd_venda, 
iv.itvvlruni preco_unit, iv.itvvlrtot total_valor_item, iv.itvvlrdcn total_desconto_item,
coalesce(iv.itvdcnfid , 0.00) desconto_fidelidade_item,
round_half_down1(coalesce(t.trndcnfid, 0.00) * (iv.itvvlrtot / t.trnvlr), 2) desconto_fidelidade_item_rat,
round_half_down1(iv.itvvlrtot / t.trnvlr, 2) fator_rateio,
sum(coalesce(ivdMot.itdvlr, 0.00)) + sum(coalesce(ivdMan.itdvlr, 0.00)) + sum(coalesce(ivdSub.itdvlr, 0.00)) + sum(coalesce(ivdFid.itdvlr, 0.00)) det_total_descontos,
sum(coalesce(ivdMot.itdvlr, 0.00)) det_desconto_motor,
sum(coalesce(ivdFid.itdvlr, 0.00)) det_desconto_fideli,
sum(coalesce(ivdMan.itdvlr, 0.00)) det_desconto_man_item,
sum(coalesce(ivdSub.itdvlr, 0.00)) det_desconto_man_sub
from transacao t 
inner join item_venda iv on t.trndat = iv.trndat and t.lojcod = iv.lojcod and t.trnseq = iv.trnseq and t.cxanum = iv.cxanum 
inner join pessoa_loja pl on t.lojcod = pl.loj_codigo 
inner join caixa c on t.lojcod = c.lojcod and t.cxanum = c.cxanum
left join pedido_venda pv on t.pva_id = pv.pva_id 
left join (select ivd.itv_id id, sum(coalesce(ivd.itdvlr, 0.00)) itdvlr from item_venda_desconto ivd where ivd.itdatv is true and ivd.itdori = '0' group by ivd.itv_id) ivdMan on iv.id = ivdMan.id
left join (select ivd.itv_id id, sum(coalesce(ivd.itdvlr, 0.00)) itdvlr from item_venda_desconto ivd where ivd.itdatv is true and ivd.itdori = '1' group by ivd.itv_id) ivdMot on iv.id = ivdMot.id
left join (select ivd.itv_id id, sum(coalesce(ivd.itdvlr, 0.00)) itdvlr from item_venda_desconto ivd where ivd.itdatv is true and ivd.itdori = '2' group by ivd.itv_id) ivdSub on iv.id = ivdSub.id
left join (select ivd.itv_id id, sum(coalesce(ivd.itdvlr, 0.00)) itdvlr from item_venda_desconto ivd where ivd.itdatv is true and ivd.itdori = '3' group by ivd.itv_id) ivdFid on iv.id = ivdFid.id
where t.trntip = '1' 
and iv.itvtip != '2'
and t.trndat between {} and {}
and pl.loj_codigo_dominio = 'BOT'
and c.cxa_frente_loja != 'MOBSHOP'
AND t.id_externo IS null 
and trim(iv.itvseq) != ''
and not exists (select * from reprocessamento_descontos_bso where identificador = t.identificador and status = 'SUCESSO')
group by t.lojcod, pl.loj_codigo_dominio, t.trnseq, t.trndat, t.cxanum, pv.pva_origem, c.cxa_frente_loja, t.trnvlr, t.trndcn, t.trndcnfid, iv.id, iv.itvseq
order by t.lojcod, t.trndat, t.trnseq, t.cxanum, iv.itvseq;