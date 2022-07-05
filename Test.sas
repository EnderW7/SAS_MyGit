*————清空日志、输出窗口、结果查看器、work逻辑库————;
dm 'log; clear;';   /*日志*/
dm 'output; clear;';   /*输出窗口*/
dm 'odsresult; clear;';   /*结果查看器*/
dm 'lst;clear;';

proc datasets library = Work nolist nodetails kill;run;quit;   /*work逻辑库中的s数据集*/
*————以上为系统清理程序————;

*2021-523-00CH1 Manul Check;
*————↓以下为每次Running需要修改的参数↓————;
%let New = C:\邬旻昊\Projects\2021-523-00CH1\Data Clean\RawData\20210608;
%let Old = C:\邬旻昊\Projects\2021-523-00CH1\Data Clean\RawData\20210508;
%let Path_Export = C:\邬旻昊\Projects\2021-523-00CH1\Data Clean\Output;
*————↑以上为每次Running需要修改的参数↑————;

libname RAW "&New";
libname OUTPUT "&New.\Output\Manual_Check";
libname COMP_NEW "&New.\Compare\Manual_Check";
libname COMP_OLD "&Old.\Compare\Manual_Check";

%macro Kp_Sub(Libname, Domain, Var_kp, Name);   /*主体keep宏*/
data &Libname..&Name.;
	set Raw.&Domain.;
	keep project Subject Site SiteNumber InstanceName DataPageName RecordPosition &Var_kp.;
run;
%mend Kp_Sub;

%macro Kp_Obj(Libname, Domain, Var_kp, Valist);   /*Mapping客体keep宏*/
data &Libname..&Domain.;
	set Raw.&Domain.;
	keep &Valist. &Var_kp.;
run;
%mend Kp_Obj;

%macro Mapping1(Sub, Obj, Valist, Name);   /*1个主体与1个客体Merge宏*/
proc sort data = &Sub.;by &Valist.;run;
proc sort data = &Obj.;by &Valist.;run;

data Output.&Name.;
	merge &Sub.(in = a) &Obj.;
	by &Valist.;
	if a;
run;
%mend Mapping1;

%macro Mapping2(Sub, Obj1, Obj2, Valist, Name);   /*1个主体与2个客体Merge宏*/
proc sort data = &Sub.;by &Valist.;run;
proc sort data = &Obj1.;by &Valist.;run;
proc sort data = &Obj2.;by &Valist.;run;

data Output.&Name.;
	merge &Sub.(in = a) &Obj1. &Obj2.;
	by &Valist.;
	if a;
run;
%mend Mapping2;

**针对常用于Cross Check的Domain(IE MH AE)，先进行受试者唯一匹配，方便后续Mapping;
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
***主体为 IE;
%Kp_Sub(OUTPUT, Ie, IEYN IECAT IENO, IE)

**Pr2;
***主体为 Pr2: keep PRDAT2 PRDOSE, Mapping客体为 Ie: keep IECAT IENO;
%Kp_Sub(Work, Pr2, PRDAT2 PRDOSE, Pr2)
%Mapping1(Pr2, IE_U, Subject, Pr2)

**Pr;
***主体为 Pr: keep PRTRT PRSTDAT PRPINDC, Mapping客体为 Ie: keep IECAT IENO, MH: keep MHTERM MHSTDAT MHENDAT;
%Kp_Sub(Work, Pr, PRTRT PRSTDAT PRPINDC, Pr)
%Mapping2(Pr, IE_U, MH_U, Subject, Pr)

**Su;
***主体为 Su: keep SUSTDAT2 SUENDAT2 SUDSTXT2 SUSTDAT1 SUENDAT1 SUDSTXT1, Mapping客体为 Ie: keep IECAT IENO;
%Kp_Sub(Work, Su, SUSTDAT2 SUENDAT2 SUDSTXT2 SUSTDAT1 SUENDAT1 SUDSTXT1, Su)
%Mapping1(Su, IE_U, Subject, Su)

**Prx;
***主体为 Prx;
%Kp_Sub(OUTPUT, Prx, PRXRES PRXCSDES, Prx)

**Pe1;
***主体为 Pe1;
%Kp_Sub(OUTPUT, Pe1, PECLSIG PECSDESC, Pe1)

**Dm;
***主体为 Dm: keep BEAR, Mapping客体为 Lbp: keep InstanceName LBPYN LBPRES LBPRES_UN;
%Kp_Sub(Work, Dm, BEAR, Dm)
%Kp_Obj(Work, Lbp, InstanceName LBPYN LBPRES LBPRES_UN, Subject)
%Mapping1(Dm, Lbp, Subject, Dm)

**MH;
***主体为 MH: keep MHTERM MHSTDAT MHENDAT, Mapping客体为 AE: keep AETERM AESTDAT AEENDAT;
%Kp_Sub(Work, MH, MHTERM MHSTDAT MHENDAT, MH)
%Mapping1(MH, AE_U, Subject, MH)

**Vs1;
***主体为 Vs1: keep VSORRES VSTEST VSRES VSRESU, Mapping客体为 IE_U, AE_U;
%Kp_Sub(Work, Vs1, VSORRES VSTEST VSRES VSRESU, Vs1)
%Mapping2(Vs1, IE_U, AE_U, Subject, Vs1)

**EG;
***主体为 EG;
%Kp_Sub(OUTPUT, EG, EGDAT EGTIM QTCF RESULT EGABCS, EG)

**Pk;
***主体为 Pk;
%Kp_Sub(OUTPUT, Pk, PKTEST PKDAT PKTIM, Pk)

**PK RECON ;
***主体为 Pk: keep PKTEST PKDAT PKTIM PKND, Mapping客体为 供应商外部数据;


**Rm;
***主体为 Rm;
%Kp_Sub(OUTPUT, Rm, RMNUM, Rm)

**Ex;
***主体为 Ex;
%Kp_Sub(OUTPUT, Ex, EXNO EXSTTIM BRFSTTIM, Ex)

**AE_01;
***主体为 AE: keep AETERM AESTDAT AESTTIM AEENDAT AEENDTIM AEREL AEACN AEACNCM AEOUT AESER, Mapping客体为 EX: keep EXTRT EXSTDAT EXSTTIM, CM: keep CMTRT CMSTDAT CMENDAT CMAE1 CMAE1_STD CMAE2 CMAE2_STD;
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


























