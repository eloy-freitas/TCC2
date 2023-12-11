create schema dw;

create schema stg;

create table dw.d_curso(
	sk_curso int primary key,
	cd_curso int,
	no_curso varchar(200) not null,
	no_centro_academico varchar(20),
	dt_carga timestamp
); 

insert into dw.d_curso(sk_curso, cd_curso, no_curso, no_centro_academico, dt_carga) 
values
    (-1, -1, 'Não Informado', 'Não Informado', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, -2, 'Não Aplicável', 'Não Aplicável', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, -3, 'Desconhecido', 'Desconhecido', to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   

comment on column dw.d_curso.sk_curso is 'Surrogate key da dimensão curso (Coluna gerada)';
comment on column dw.d_curso.cd_curso is 'Código do curso';
comment on column dw.d_curso.no_curso is 'Nome do curso';
comment on column dw.d_curso.no_centro_academico is 'Nome do centro acadêmico';
comment on column dw.d_curso.dt_carga is 'Data de atualização do registro';

	

create table dw.d_tipo_cota(
	sk_tipo_cota int primary key,
	ds_tipo_cota varchar(200) not null,
	dt_carga timestamp
); 


insert into dw.d_tipo_cota(sk_tipo_cota, ds_tipo_cota, dt_carga) 
values
    (-1, 'Não Informado', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, 'Não Aplicável', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, 'Desconhecido', to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   

comment on column dw.d_tipo_cota.sk_tipo_cota is 'Surrogate key da dimensão tipo de cota (Coluna gerada)';
comment on column dw.d_tipo_cota.ds_tipo_cota is 'Descrição da forma de ingresso';
comment on column dw.d_tipo_cota.dt_carga is 'Data de atualização do registro';

create table dw.d_forma_evasao(
	sk_forma_evasao int primary key,
	ds_forma_evasao varchar(200) not null,
	dt_carga timestamp
);


insert into dw.d_forma_evasao(sk_forma_evasao, ds_forma_evasao, dt_carga) 
values
    (-1, 'Não Informado', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, 'Não Aplicável', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, 'Desconhecido', to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   

comment on column dw.d_forma_evasao.sk_forma_evasao is 'Surrogate key da dimensão forma de evasão (Coluna gerada)';
comment on column dw.d_forma_evasao.ds_forma_evasao is 'Descrição da forma de evasão';
comment on column dw.d_forma_evasao.dt_carga is 'Data de atualização do registro';


create table dw.d_forma_ingresso(
	sk_forma_ingresso int primary key,
	ds_forma_ingresso varchar(200) not null,
	dt_carga timestamp
);


comment on column dw.d_forma_ingresso.sk_forma_ingresso is 'Surrogate key da dimensão forma de ingresso (Coluna gerada)';
comment on column dw.d_forma_ingresso.ds_forma_ingresso is 'Descrição da forma de ingresso';
comment on column dw.d_forma_ingresso.dt_carga is 'Data de atualização do registro';


insert into dw.d_forma_ingresso(sk_forma_ingresso, ds_forma_ingresso, dt_carga) 
values
    (-1, 'Não Informado', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, 'Não Aplicável', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, 'Desconhecido', to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   

CREATE TABLE dw.d_pessoa (
	sk_pessoa int primary key,
	nu_cpf varchar(30),
	no_pessoa varchar(200),
	ds_genero varchar(200),
	dt_nascimento date NULL,
	ds_deficiencia varchar(200),
	ds_etinia varchar(200),
	ds_estado_civil varchar(200),
	no_municipio_naturalidade varchar(200),
	no_municipio_origem varchar(200),
	cd_uf varchar(2),
	dt_carga timestamp
);

	
insert into dw.d_pessoa(sk_pessoa, nu_cpf, no_pessoa, ds_genero, dt_nascimento, ds_deficiencia, ds_etinia, ds_estado_civil, no_municipio_naturalidade, no_municipio_origem, cd_uf, dt_carga) 
values
    (-1, 'Não Informado','Não Informado','Não Informado',to_timestamp('01/01/1900', 'dd/mm/yyyy'),'Não Informado','Não Informado','Não Informado','Não Informado','Não Informado','-1', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, 'Não Aplicável','Não Aplicável','Não Aplicável',to_timestamp('01/01/1900', 'dd/mm/yyyy'),'Não Aplicável','Não Aplicável','Não Aplicável','Não Aplicável','Não Aplicável','-2', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, 'Desconhecido', 'Desconhecido', 'Desconhecido',to_timestamp('01/01/1900', 'dd/mm/yyyy'),'Desconhecido','Desconhecido','Desconhecido','Desconhecido','Desconhecido','-3',to_timestamp('01/01/1900', 'dd/mm/yyyy'));

	
comment on column dw.d_pessoa.sk_pessoa is 'Surrogate key da dimensão pessoa (Coluna gerada)';
comment on column dw.d_pessoa.nu_cpf is 'Número de CPF';
comment on column dw.d_pessoa.no_pessoa is 'Nome da pessoa';
comment on column dw.d_pessoa.ds_genero is 'Descrição do gênero declarado';
comment on column dw.d_pessoa.dt_nascimento is 'Data de nascimento';
comment on column dw.d_pessoa.ds_deficiencia is 'Descrição da deficiência declarada';
comment on column dw.d_pessoa.ds_etinia is 'Descrição da etinia declarada';
comment on column dw.d_pessoa.ds_estado_civil is 'Descrição do estado civíl';
comment on column dw.d_pessoa.no_municipio_naturalidade is 'Nome do muncípio de naturalidade';
comment on column dw.d_pessoa.no_municipio_origem is 'Nome do muncípio de origem';
comment on column dw.d_pessoa.cd_uf is 'Código da Unidade Federativa';
comment on column dw.d_pessoa.dt_carga is 'Data de atualização do registro';


create table dw.d_periodos_cursados(
	sk_periodos_cursados int primary key,
	nu_periodos_cursados int,
	dt_carga timestamp
);

	
insert into dw.d_periodos_cursados(sk_periodos_cursados, nu_periodos_cursados, dt_carga) 
values
    (-1, -1, to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, -2, to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, -3, to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   
	
comment on column dw.d_periodos_cursados.sk_periodos_cursados is 'Surrogate key da dimensão de periodos cursados (Coluna gerada)';
comment on column dw.d_periodos_cursados.nu_periodos_cursados is 'Quantidade de periodos cursados até a evasão';
comment on column dw.d_periodos_cursados.dt_carga is 'Data de atualização do registro';


create table dw.d_calendario (
	sk_calendario int primary key,
	nu_ano_referencia int,
	ds_periodo_referencia varchar(30),
	dt_carga timestamp
);


insert into dw.d_calendario(sk_calendario, nu_ano_referencia, ds_periodo_referencia, dt_carga) 
values
    (-1, -1, 'Não Informado', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-2, -2, 'Não Aplicável', to_timestamp('01/01/1900', 'dd/mm/yyyy')),
    (-3, -3, 'Desconhecido', to_timestamp('01/01/1900', 'dd/mm/yyyy')); 
   

comment on column dw.d_calendario.sk_calendario is 'Surrogate key da dimensão de periodos cursados (Coluna gerada)';
comment on column dw.d_calendario.nu_ano_referencia is 'Número do ano';
comment on column dw.d_calendario.ds_periodo_referencia is 'Descrição do período';
comment on column dw.d_calendario.dt_carga is 'Data de atualização do registro';


create table dw.f_situacao_matricula (
	sk_pessoa int,
	sk_tipo_cota int,
	sk_forma_ingresso int,
	sk_ano_evasao int,
	sk_ano_ingresso int,
	sk_curso int,
	sk_forma_evasao int,
	sk_periodos_cursados int,
	nu_matricula text,
	nu_cra float,
	nu_crn float,
	fl_cotista text,
	nu_periodos int,
	nu_maximo_periodos int,
	nu_ch_total_curso int,
	nu_versao varchar,
	dt_carga timestamp
);


comment on column dw.f_situacao_matricula.sk_curso is 'Surrogate key da dimensão curso (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_forma_evasao is 'Surrogate key da dimensão forma de evasão (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_forma_ingresso is 'Surrogate key da dimensão forma de ingresso (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_tipo_cota is 'Surrogate key da dimensão tipo de cota (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_pessoa is 'Surrogate key da dimensão pessoa (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_ano_evasao is 'Surrogate key da dimensão calendário, representando o ano de evasão (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_ano_ingresso is 'Surrogate key da dimensão calendário, representando o ano de ingresso (Coluna gerada)';
comment on column dw.f_situacao_matricula.sk_periodos_cursados is 'Surrogate key da dimensão de periodos cursados (Coluna gerada)';
comment on column dw.f_situacao_matricula.nu_matricula is 'Número de matrícula do discente';
comment on column dw.f_situacao_matricula.nu_cra is 'Valor do Coeficiente de Rendimento do Aluno';
comment on column dw.f_situacao_matricula.nu_crn is 'Valor do Coeficiente de Rendimento Normalizado';
comment on column dw.f_situacao_matricula.fl_cotista is 'Flag de identificação de cotista';
comment on column dw.f_situacao_matricula.nu_periodos is 'Quantidade de periodos do curso';
comment on column dw.f_situacao_matricula.nu_maximo_periodos is 'Quantidade máxima de períodos';
comment on column dw.f_situacao_matricula.nu_ch_total_curso is 'Carga horária total do curso';
comment on column dw.f_situacao_matricula.nu_versao is 'Versão do curso';
comment on column dw.f_situacao_matricula.dt_carga is 'Data de atualização do registro';


create or replace view dw.v_ds_evasao as (
select 
	dp.no_pessoa 
	, dp.ds_genero 
	, dp.dt_nascimento 
	, dp.ds_deficiencia 
	, dp.ds_etinia 
	, dp.ds_estado_civil 
	, dp.no_municipio_origem
	, dp.no_municipio_naturalidade 
	, dp.cd_uf 
	, dc.cd_curso 
	, dc.no_curso 
	, dfe.ds_forma_evasao 
	, dfi.ds_forma_ingresso 
	, dtc.ds_tipo_cota 
	, f.nu_matricula 
	, d_ingresso.nu_ano_referencia  nu_ano_ingresso 
	, d_ingresso.ds_periodo_referencia  ds_periodo_ingresso 
	, f.nu_cra 
	, f.nu_crn 
	, f.fl_cotista
	, d_evasao.nu_ano_referencia  nu_ano_evasao 
	, d_evasao.ds_periodo_referencia  ds_periodo_evasao 
	, f.nu_periodos 
	, f.nu_maximo_periodos 
	, f.dt_carga 
	, case when dfe.ds_forma_evasao not in ('SEM EVASÃO', 'FORMADO') then 1
		else 0
	end fl_evasao
	, case when dfe.ds_forma_evasao not in ('SEM EVASÃO') then 1
		else 0
	end fl_evasao_diplomados
	, case when dfe.ds_forma_evasao = 'FORMADO' then 1
		else 0
	end fl_diplomado
	, dc.no_centro_academico
	, dpc.nu_periodos_cursados 
from dw.f_situacao_matricula f
left join dw.d_pessoa dp 
	on f.sk_pessoa = dp.sk_pessoa 
left join dw.d_curso dc 
	on f.sk_curso = dc.sk_curso 
left join dw.d_forma_evasao dfe 
	on f.sk_forma_evasao = dfe.sk_forma_evasao 
left join dw.d_forma_ingresso dfi 
	on f.sk_forma_ingresso = dfi.sk_forma_ingresso 
left join dw.d_tipo_cota dtc 
	on f.sk_tipo_cota = dtc.sk_tipo_cota
left join dw.d_calendario d_evasao
	on f.sk_ano_evasao = d_evasao.sk_calendario 
left join dw.d_calendario d_ingresso
	on f.sk_ano_ingresso  = d_ingresso.sk_calendario 
left join dw.d_periodos_cursados dpc 
	on f.sk_periodos_cursados = dpc.sk_periodos_cursados
);


comment on column dw.v_ds_evasao.no_pessoa is 'Nome da pessoa';
comment on column dw.v_ds_evasao.ds_genero is 'Descrição do gênero declarado';
comment on column dw.v_ds_evasao.dt_nascimento is 'Data de nascimento';
comment on column dw.v_ds_evasao.ds_deficiencia is 'Descrição da deficiência declarada';
comment on column dw.v_ds_evasao.ds_etinia is 'Descrição da etinia declarada';
comment on column dw.v_ds_evasao.ds_estado_civil is 'Descrição do estado civíl';
comment on column dw.v_ds_evasao.no_municipio_naturalidade is 'Nome do muncípio de naturalidade';
comment on column dw.v_ds_evasao.no_municipio_origem is 'Nome do muncípio de origem';
comment on column dw.v_ds_evasao.cd_uf is 'Código da Unidade Federativa';
comment on column dw.v_ds_evasao.cd_curso is 'Código do curso';
comment on column dw.v_ds_evasao.no_curso is 'Nome do curso';
comment on column dw.v_ds_evasao.no_centro_academico is 'Nome do centro acadêmico';
comment on column dw.v_ds_evasao.ds_tipo_cota is 'Descrição da forma de ingresso';
comment on column dw.v_ds_evasao.ds_forma_evasao is 'Descrição da forma de evasão';
comment on column dw.v_ds_evasao.ds_forma_ingresso is 'Descrição da forma de ingresso';
comment on column dw.v_ds_evasao.nu_periodos_cursados is 'Quantidade de periodos cursados até a evasão';
comment on column dw.v_ds_evasao.nu_ano_ingresso is 'Número do ano de ingresso';
comment on column dw.v_ds_evasao.ds_periodo_ingresso is 'Descrição do período de ingresso';
comment on column dw.v_ds_evasao.nu_ano_evasao is 'Número do ano de evasão';
comment on column dw.v_ds_evasao.ds_periodo_evasao is 'Descrição do período de evasão';
comment on column dw.v_ds_evasao.nu_matricula is 'Número de matrícula do discente';
comment on column dw.v_ds_evasao.nu_cra is 'Valor do Coeficiente de Rendimento do Aluno';
comment on column dw.v_ds_evasao.nu_crn is 'Valor do Coeficiente de Rendimento Normalizado';
comment on column dw.v_ds_evasao.fl_cotista is 'Flag de identificação de cotista';
comment on column dw.v_ds_evasao.fl_evasao is 'Flag de identificação de alunos que tiveram alguma forma de evasão';
comment on column dw.v_ds_evasao.nu_periodos is 'Quantidade de periodos do curso';
comment on column dw.v_ds_evasao.nu_maximo_periodos is 'Quantidade máxima de períodos';
comment on column dw.v_ds_evasao.fl_evasao is 'Flag para facilitar a identificação de uma evasão';
comment on column dw.v_ds_evasao.fl_diplomado is 'Flag para facilitar a identificação de uma diplomação';
comment on column dw.v_ds_evasao.fl_evasao_diplomados is 'Flag para facilitar a identificação de diplomados';
comment on column dw.v_ds_evasao.dt_carga is 'Data de atualização do registro';


CREATE TABLE stg.stg_relatorio (
	"MATR_ALUNO" text NULL,
	"NOME_ALUNO" text NULL,
	"SEXO" text NULL,
	"DT_NASCIMENTO" text NULL,
	"ESTADO_CIVIL" text NULL,
	"ETNIA" text NULL,
	"COD_CURSO" int8 NULL,
	"NOME_CURSO" text NULL,
	"ANO_INGRESSO" int8 NULL,
	"FORMA_INGRESSO" text NULL,
	"PERIODO_INGRESSO" text NULL,
	"TURNO_ALUNO_ITEM" int8 NULL,
	"TURNO" text NULL,
	"ANO_EVASAO" float8 NULL,
	"PERIODO_EVASAO" text NULL,
	"FORMA_EVASAO" text NULL,
	"NUM_PERIODOS" int8 NULL,
	"NUM_MAX_PERIODOS" int8 NULL,
	"CH_TOTAL_CURSO" int8 NULL,
	"DESCR_MAIL" text NULL,
	"COTISTA" text NULL,
	"CEP" text NULL,
	"TIPO_LOGRADOURO" text NULL,
	"RUA" text NULL,
	"NUMERO" text NULL,
	"COMPLEMENTO" text NULL,
	"BAIRRO" text NULL,
	"MUNICIPIO" text NULL,
	"ESTADO" text NULL,
	"PAIS" text NULL,
	"UF" text NULL,
	"CPF" text NULL,
	"RG" text NULL,
	"RG_ORGAO_EMISSOR" text NULL,
	"RG_ESTADO" text NULL,
	"CRN" text NULL,
	"CRA" text NULL,
	"TIPO_COTA" text NULL,
	"NATURALIDADE" text NULL,
	"UF_NATURALIDADE" text NULL,
	"DEFICIENCIA" text NULL,
	"NUM_VERSAO" text NULL,
	"SITUACAO_VERSAO" text NULL,
	"QTDE_TRANCAMENTOS" text NULL,
	"NOME_CURSO_DIPLOMA" text NULL,
	"MODALIDADE" text NULL,
	"NOME_CENTRO" text NULL,
	"NOME_CAMPUS" text NULL,
	"NIVEL_CURSO" text NULL
);


create or replace view  stg.v_ds_stg_relatorio as (
	with stage as (
		select  
			--pessoa
			case "SEXO"
				when 'M' then 'MASCULINO'
				when 'F' then 'FEMININO'
				when 'N' then 'Não Informado'
				else "SEXO"
			end ds_genero
			, to_date("DT_NASCIMENTO", 'yyyy-mm-dd') dt_nascimento
			, upper(trim("NOME_ALUNO")) no_pessoa
			, upper(trim("ESTADO_CIVIL")) ds_estado_civil
			, upper(trim("ETNIA")) ds_etinia
			, upper(trim("DEFICIENCIA")) ds_deficiencia
			, upper(trim("NATURALIDADE")) no_municipio_naturalidade
			, upper(trim("UF_NATURALIDADE")) cd_uf_naturalidade
			, upper(trim("RG_ESTADO")) nu_rg_estado
			, upper(trim("RG_ORGAO_EMISSOR")) no_rg_orgao_emissor 
			, upper(trim("RG")) nu_rg
			, upper(trim("CPF")) nu_cpf 
			, upper(trim("UF")) cd_uf
			, upper(trim("PAIS")) no_pais 
			, upper(trim("ESTADO")) no_estado
			, upper(trim("MUNICIPIO")) no_municipio_origem
			, upper(trim("BAIRRO")) no_bairro
			, upper(trim("COMPLEMENTO")) ds_complemento
			, upper(trim("NUMERO")) nu_residencia
			, upper(trim("RUA")) ds_logradouro
			, upper(trim("TIPO_LOGRADOURO")) ds_tipo_logradouro
			, upper(trim("CEP")) nu_cep 
			, upper(trim("DESCR_MAIL")) ds_email 
			--curso
			, "COD_CURSO" cd_curso
			, upper(trim("NOME_CURSO")) no_curso
			, "NUM_PERIODOS" nu_periodos
			, "NUM_MAX_PERIODOS" nu_maximo_periodos 
			, "CH_TOTAL_CURSO" nu_ch_total_curso
			, upper(trim("SITUACAO_VERSAO")) ds_situacao_versao
			, upper(trim("NUM_VERSAO")) nu_versao
			--matricula
			, regexp_replace(upper(trim("MATR_ALUNO")), 'X','') nu_matricula
			, "ANO_INGRESSO" nu_ano_ingresso
			, upper(trim("FORMA_INGRESSO")) ds_forma_ingresso 
			, upper(trim("PERIODO_INGRESSO")) ds_periodo_ingresso 
			, "TURNO_ALUNO_ITEM"  nu_turno_aluno
			, upper(trim("TURNO")) ds_turno
			, coalesce(cast("CRA" as float), 0) nu_cra
			, coalesce(cast("CRN" as float), 0) nu_crn
			--cota
			, case upper(trim("COTISTA"))
				when 'S' then 'Cotista'
				when 'N' then 'Não Cotista'
				else upper(trim("COTISTA"))
			end fl_cotista
			,upper(trim("TIPO_COTA")) ds_tipo_cota
			--evasao
			, upper(trim("PERIODO_EVASAO")) ds_periodo_evasao
			, upper(trim("FORMA_EVASAO")) ds_forma_evasao
			, "ANO_EVASAO" nu_ano_evasao
			, case upper("NOME_CENTRO")
				when 'CENTRO DE CIÊNCIAS EXATAS, NATURAIS E DA SAÚDE' then 'CCENS'
				when 'CENTRO DE CIÊNCIAS AGRÁRIAS E ENGENHARIAS' then 'CCAE'
			end no_centro_academico
		from stg.stg_relatorio sr 
	)
	select distinct
		no_pessoa
		, ds_estado_civil
		, ds_etinia
		, ds_deficiencia
		, no_municipio_naturalidade
		, cd_uf_naturalidade
		, nu_rg_estado
		, no_rg_orgao_emissor 
		, nu_rg
		, nu_cpf 
		, no_pais 
		, no_estado
		, no_municipio_origem
		, nu_cep 
		, ds_email 
		, cd_curso
		, no_curso
		, nu_periodos
		, nu_maximo_periodos 
		, nu_ch_total_curso
		, ds_situacao_versao
		, nu_versao
		, nu_matricula
		, nu_ano_ingresso
		, ds_forma_ingresso 
		, ds_periodo_ingresso 
		, nu_turno_aluno
		, ds_turno
		, nu_cra
		, nu_crn
		, fl_cotista
		, ds_tipo_cota
		, ds_periodo_evasao
		, ds_forma_evasao
		, nu_ano_evasao
		, ds_genero
		, dt_nascimento
		, no_centro_academico
		, cd_uf
	from stage	
);