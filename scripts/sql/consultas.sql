select count(nu_matricula) from f_situacao_matricula fsm --1013
where fl_evasao = 1

select dc.no_curso, count(nu_matricula)  
from f_situacao_matricula f
inner join d_curso dc 
	on f.sk_curso = dc.sk_curso 
where f.fl_evasao = 1
group by dc.no_curso 
order by 2 desc

select dfe.ds_forma_evasao, count(nu_matricula)  
from f_situacao_matricula f
inner join d_forma_evasao dfe 
	on f.sk_forma_evasao = dfe.sk_forma_evasao 
where f.fl_evasao = 1
group by dfe.ds_forma_evasao 
order by 2 desc

select dfi.ds_forma_ingresso, count(nu_matricula) 
from f_situacao_matricula f
inner join d_forma_ingresso dfi 
	on f.sk_forma_ingresso = dfi.sk_forma_ingresso 
where f.fl_evasao = 1
group by dfi.ds_forma_ingresso  
order by 2 desc
	
select dtc.ds_tipo_cota, count(nu_matricula) 
from f_situacao_matricula f
inner join d_tipo_cota dtc 
	on f.sk_tipo_cota = dtc.sk_tipo_cota 
where f.fl_evasao = 1
group by dtc.ds_tipo_cota 
order by 2 desc

select dp.ds_sexo, count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.ds_sexo 
order by 2 desc

select dp.ds_etinia , count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.ds_etinia
order by 2 desc

select dp.ds_deficiencia , count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.ds_deficiencia 
order by 2 desc


select dp.ds_estado_civil  , count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.ds_estado_civil
order by 2 desc

select dp.no_municipio_origem , count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.no_municipio_origem 
order by 2 desc


select dp.cd_uf  , count(nu_matricula)  
from f_situacao_matricula f
inner join d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
where f.fl_evasao = 1
group by dp.cd_uf
order by 2 desc

with dw_comments as (
	select
	    d.table_schema schema_destino,
	    d.table_name tabela_destino,
	    d.column_name coluna_destino,
	    d.data_type tipo_dado_destino,
	    pgd.description descricao
	from pg_catalog.pg_statio_all_tables st
	inner join pg_catalog.pg_description pgd 
		on pgd.objoid = st.relid
	inner join information_schema.columns d
		on pgd.objsubid = d.ordinal_position 
		and d.table_schema = st.schemaname 
		and d.table_name = st.relname
	WHERE d.table_schema = 'dw'
)
, origem as (
	select
		'stg' schema_origem,
		'v_ds_stg_relatorio' tabela_origem,
	    a.attname coluna_origem,
	    t.typname tipo_dado_origem
	FROM pg_class c
	INNER JOIN pg_attribute a ON a.attrelid = c.oid
	INNER JOIN pg_type t ON t.oid = a.atttypid
	WHERE c.relkind = 'v'
	    AND c.relname = 'v_ds_stg_relatorio'
)
select 
	d.schema_destino,
    d.tabela_destino,
    d.coluna_destino,
    d.tipo_dado_destino,
    d.descricao,
    o.schema_origem,
	o.tabela_origem,
    o.coluna_origem,
    o.tipo_dado_origem,
    (SELECT pg_get_viewdef('stg.v_ds_stg_relatorio')) view_origem
from dw_comments d
left join origem o
	on d.coluna_destino = o.coluna_origem