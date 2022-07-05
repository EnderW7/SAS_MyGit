*�������������־��������ڡ�����鿴����work�߼��⡪������;
dm 'log; clear;';   /*��־*/
dm 'output; clear;';   /*�������*/
dm 'odsresult; clear;';   /*����鿴��*/
dm 'lst;clear;';

proc datasets library = Work nolist nodetails kill;run;quit;   /*work�߼����е�s���ݼ�*/
*������������Ϊϵͳ������򡪡�����;

*2021-523-00CH1 Manul Check;
*��������������Ϊÿ��Running��Ҫ�޸ĵĲ�������������;
%let New = C:\���F�\Projects\2021-523-00CH1\Data Clean\RawData\20210608;
%let Old = C:\���F�\Projects\2021-523-00CH1\Data Clean\RawData\20210508;
%let Path_Export = C:\���F�\Projects\2021-523-00CH1\Data Clean\Output;
*��������������Ϊÿ��Running��Ҫ�޸ĵĲ�������������;

libname RAW "&New";
libname OUTPUT "&New.\Output\Manual_Check";
libname COMP_NEW "&New.\Compare\Manual_Check";
libname COMP_OLD "&Old.\Compare\Manual_Check";

%macro Kp_Sub(Libname, Domain, Var_kp, Name);   /*����keep��*/
data &Libname..&Name.;
	set Raw.&Domain.;
	keep project Subject Site SiteNumber InstanceName DataPageName RecordPosition &Var_kp.;
run;
%mend Kp_Sub;

%macro Kp_Obj(Libname, Domain, Var_kp, Valist);   /*Mapping����keep��*/
data &Libname..&Domain.;
	set Raw.&Domain.;
	keep &Valist. &Var_kp.;
run;
%mend Kp_Obj;

%macro Mapping1(Sub, Obj, Valist, Name);   /*1��������1������Merge��*/
proc sort data = &Sub.;by &Valist.;run;
proc sort data = &Obj.;by &Valist.;run;

data Output.&Name.;
	merge &Sub.(in = a) &Obj.;
	by &Valist.;
	if a;
run;
%mend Mapping1;

%macro Mapping2(Sub, Obj1, Obj2, Valist, Name);   /*1��������2������Merge��*/
proc sort data = &Sub.;by &Valist.;run;
proc sort data = &Obj1.;by &Valist.;run;
proc sort data = &Obj2.;by &Valist.;run;

data Output.&Name.;
	merge &Sub.(in = a) &Obj1. &Obj2.;
	by &Valist.;
	if a;
run;
%mend Mapping2;

**��Գ�����Cross Check��Domain(IE MH AE)���Ƚ���������Ψһƥ�䣬�������Mapping;
%Kp_Obj(Work, IE, IEYN IECAT IENO, Subject)
%Kp_Obj(Work, MH, MHTERM MHSTDAT MHENDAT, Subject)
%Kp_Obj(Work, AE, AETERM AESTDAT AEENDAT, Subject)

proc sort data = IE;by Subject;run;
proc sort data = MH;by Subject;run;
proc sort data = AE;by Subject;run;

data IE_U;
	set IE;
	by Subject;
	Criteria = cats(IECAT, IENO);
	retain IEList;
	length IEList $1000.;
	if first.Subject then IEList = Criteria;
	else IEList = catx("#n", IEList, Criteria);
	if last.Subject;
	drop IECAT IENO Criteria;
run;

data MH_U;
	set MH;
	by Subject;
	MH_U = cats(MHTERM, "-", MHSTDAT, "-", MHENDAT);
	retain MHList;
	length MHList $1000.;
	if first.Subject then MHList = MH_U;
	else MHList = catx("#n", MHList, MH_U);
	if last.Subject;
	drop MHTERM MHSTDAT MHENDAT MH_U;
run;

data AE_U;
	set AE;
	by Subject;
	AE_U = cats(AETERM, "-", AESTDAT, "-", AEENDAT);
	retain AEList;
	length AEList $1000.;
	if first.Subject then AEList = AE_U;
	else AEList = catx("#n", AEList, AE_U);
	if last.Subject;
	drop AETERM AESTDAT AEENDAT AE_U;
run;

ods escapechar = "#";

**IE;
***����Ϊ IE;
%Kp_Sub(OUTPUT, Ie, IEYN IECAT IENO, IE)

**Pr2;
***����Ϊ Pr2: keep PRDAT2 PRDOSE, Mapping����Ϊ Ie: keep IECAT IENO;
%Kp_Sub(Work, Pr2, PRDAT2 PRDOSE, Pr2)
%Mapping1(Pr2, IE_U, Subject, Pr2)

**Pr;
***����Ϊ Pr: keep PRTRT PRSTDAT PRPINDC, Mapping����Ϊ Ie: keep IECAT IENO, MH: keep MHTERM MHSTDAT MHENDAT;
%Kp_Sub(Work, Pr, PRTRT PRSTDAT PRPINDC, Pr)
%Mapping2(Pr, IE_U, MH_U, Subject, Pr)

**Su;
***����Ϊ Su: keep SUSTDAT2 SUENDAT2 SUDSTXT2 SUSTDAT1 SUENDAT1 SUDSTXT1, Mapping����Ϊ Ie: keep IECAT IENO;
%Kp_Sub(Work, Su, SUSTDAT2 SUENDAT2 SUDSTXT2 SUSTDAT1 SUENDAT1 SUDSTXT1, Su)
%Mapping1(Su, IE_U, Subject, Su)

**Prx;
***����Ϊ Prx;
%Kp_Sub(OUTPUT, Prx, PRXRES PRXCSDES, Prx)

**Pe1;
***����Ϊ Pe1;
%Kp_Sub(OUTPUT, Pe1, PECLSIG PECSDESC, Pe1)

**Dm;
***����Ϊ Dm: keep BEAR, Mapping����Ϊ Lbp: keep InstanceName LBPYN LBPRES LBPRES_UN;
%Kp_Sub(Work, Dm, BEAR, Dm)
%Kp_Obj(Work, Lbp, InstanceName LBPYN LBPRES LBPRES_UN, Subject)
%Mapping1(Dm, Lbp, Subject, Dm)

**MH;
***����Ϊ MH: keep MHTERM MHSTDAT MHENDAT, Mapping����Ϊ AE: keep AETERM AESTDAT AEENDAT;
%Kp_Sub(Work, MH, MHTERM MHSTDAT MHENDAT, MH)
%Mapping1(MH, AE_U, Subject, MH)

**Vs1;
***����Ϊ Vs1: keep VSORRES VSTEST VSRES VSRESU, Mapping����Ϊ IE_U, AE_U;
%Kp_Sub(Work, Vs1, VSORRES VSTEST VSRES VSRESU, Vs1)
%Mapping2(Vs1, IE_U, AE_U, Subject, Vs1)

**EG;
***����Ϊ EG;
%Kp_Sub(OUTPUT, EG, EGDAT EGTIM QTCF RESULT EGABCS, EG)

**Pk;
***����Ϊ Pk;
%Kp_Sub(OUTPUT, Pk, PKTEST PKDAT PKTIM, Pk)

**PK RECON ;
***����Ϊ Pk: keep PKTEST PKDAT PKTIM PKND, Mapping����Ϊ ��Ӧ���ⲿ����;


**Rm;
***����Ϊ Rm;
%Kp_Sub(OUTPUT, Rm, RMNUM, Rm)

**Ex;
***����Ϊ Ex;
%Kp_Sub(OUTPUT, Ex, EXNO EXSTTIM BRFSTTIM, Ex)

**AE_01;
***����Ϊ AE: keep AETERM AESTDAT AESTTIM AEENDAT AEENDTIM AEREL AEACN AEACNCM AEOUT AESER, Mapping����Ϊ EX: keep EXTRT EXSTDAT EXSTTIM, CM: keep CMTRT CMSTDAT CMENDAT CMAE1 CMAE1_STD CMAE2 CMAE2_STD;
%Kp_Sub(Work, AE, AETERM AESTDAT AESTTIM AEENDAT AEENDTIM AEREL AEACN AEACNCM AEOUT AESER, AE)
%Kp_Obj(Work, EX, EXTRT EXSTDAT EXSTTIM, Subject)
%Kp_Obj(Work, CM, CMTRT CMSTDAT CMENDAT CMAE1 CMAE1_STD CMAE2 CMAE2_STD, Subject)

proc sort data = EX;by Subject EXSTDAT EXSTTIM;run;
proc sort data = AE;by Subject;run;
proc sort data = CM;by Subject;run;

data EX_U;
	set EX;
	by Subject;
	if last.Subject;
run;

data AE_EX;
	merge AE(in = a) EX_U;
	by Subject;
	if a;
run;

proc sql;
	create table CM01 as
	select Subject, CMTRT, CMSTDAT, CMENDAT, CMAE1 as CMAENO, CMAE1_STD as CMAENO_STD
	from CM;

	create table CM02 as
	select Subject, CMTRT, CMSTDAT, CMENDAT, CMAE2 as CMAENO, CMAE2_STD as CMAENO_STD
	from CM;
quit;

data CM_U;
	set CM01 CM02;
	CMAENO_Code = input(CMAENO_STD, best12.);
	drop CMAENO_STD;
	label CMAENO = "If AE, please specify AE" CMAENO_Code = "AE_Code";
run;

proc sort data = CM_U nodupkey;by Subject CMTRT CMSTDAT CMENDAT CMAENO CMAENO_Code;run;

proc sql;
	create table OUTPUT.AE_01 as
	select *
	from AE_EX left join CM_U
	on AE_EX.Subject = CM_U.Subject and AE_EX.RecordPosition = CM_U.CMAENO_Code;
quit;


























