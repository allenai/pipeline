#!/usr/local/bin/WolframScript -script

SetDirectory[DirectoryName[$InputFileName]]
<<ExprToHTML`

getCredit[score_,allScores_]:=Module[{max=Max[allScores]},If[score==max,N[1/Count[allScores,max]],0]]

getPRDataForFeature[fv_,solver_,featureIn_]:=Module[{feature,perQuestion,perQuestion2,pos,m,fpos=3},
  feature=(If[#==="scoreCombined",#,#<>"_"<>solver])&[featureIn];
  perQuestion=GatherBy[getColumns[fv,Append[{"questionHash","isSolution"},feature]],First];
  perQuestion2=Function[qdata,pos=Position[qdata[[All,2]],1][[1,1]];{m=Max[qdata[[All,fpos]]],getCredit[qdata[[pos,fpos]],qdata[[All,fpos]]],qdata[[1,1]]}]/@perQuestion
]

getColumns[data_,columns_]:=Module[{pos=Position[data[[1]],#,{1}]&/@columns},
  If[DeleteDuplicates[Length/@pos]=!={1},Return[$Failed]];
  Rest[data][[All,Flatten[pos]]]
]

getFeatureVectorFileFromZip[zipFile_,fvFile_]:=Module[{data1},
  data1 = If[StringMatchQ[zipFile, "*.zip"], 
    Import[zipFile,fvFile,"Numeric"->False], 
    Import[FileNameJoin[{zipFile,fvFile}], "CSV", "Numeric"->False]
  ];
  data1=DeleteCases[data1, {""...}];
  Prepend[Join[Take[#,2],ToExpression[StringReplace[#,"E"|"e"->"*^"]]&/@Drop[#,2]]&/@Rest[data1],data1[[1]]]
]

SolverComboStats[zipFile_,solvers_,allSolvers_]:=Module[{prefix,fvTrain,fvTest,rawOutput,scoreTrain,scoreTest,scoreTrainCV},
  prefix=StringJoin[Riffle[If[Length[solvers]===1,allSolvers,solvers],"-"]];
  fvTrain=getFeatureVectorFileFromZip[zipFile,prefix<>"-fvTrain.csv"];
  fvTest=getFeatureVectorFileFromZip[zipFile,prefix<>"-fvTest.csv"];
  rawOutput=If[StringMatchQ[zipFile, "*.zip"], Import[zipFile,prefix<>"-report.txt"], Import[FileNameJoin[{zipFile,prefix<>"-report.txt"}]]];
  {scoreTrain,scoreTest}=Function[fvFile,Mean[If[Length[solvers]===1,getPRDataForFeature[fvFile,solvers[[1]],"normalizedScore"],
    getPRDataForFeature[fvFile,"","scoreCombined"]][[All,2]]]]/@{fvTrain,fvTest};
  scoreTrainCV=If[Length[solvers]===1,"N/A",ToExpression[
    StringCases[rawOutput,"Logistic"~~Whitespace~~NumberString~~Whitespace~~x:NumberString:>x][[1]]]/100.];
  {"solvers"->solvers,"scoreTrain"->scoreTrain,"scoreTest"->scoreTest,"scoreTrainCV"->scoreTrainCV}
]

boldMax[scores_]:=With[{max=Max[scores/.s_String->0.]},If[#==max,Style[#,Bold],#]&/@scores]

AblationStudyReport[zipFile_String,outputFileHTML_String,outputFileCSV_String]:=Module[{files,solverSets,solvers,stats,data},
  files=If[StringMatchQ[zipFile, "*.zip"], Import[zipFile,"FileNames"], FileNameTake[#, -1]&/@FileNames["*", zipFile]];
  If[!MatchQ[files,{__String}],Return[$Failed]];
  solverSets=Union[Select[Most/@StringSplit[FileNameTake[#,-1]&/@files,"-"],Length[#]>1&]];
  solvers=Union[Flatten[solverSets]];
  stats=SolverComboStats[zipFile,#,solverSets[[-1]]]&/@Join[List/@solvers,solverSets];
  data=Prepend[Transpose[{StringJoin[Riffle[#,"+"]]&/@("solvers"/.stats),Sequence@@boldMax/@{"scoreTrain"/.stats,"scoreTrainCV"/.stats,"scoreTest"/.stats}}]/.x_Real:>ToString[NumberForm[x,3]],Style[#,Bold]&/@{"Combo","Train","Train CV","Test"}];
  {Export[outputFileHTML,XMLElement["html",{},{XMLElement["h1",{},{"Solver Combination Ablation Study"}],
    XMLElement["p",{},{XMLElement["b",{},{"Date: "}],DateString[]}],
    XMLElement["p",{},{XMLElement["b",{},{"Solvers: "}],StringJoin[Riffle[solvers,", "]]}],
    XMLElement["div",{"style"->"font-family:Arial"},Flatten[{exprToHTML[Grid[data]]}]]}],"XML"],
   Export[outputFileCSV,data/.Style[x_,___]:>x,"CSV"]}
]

replaceWithDefault[key_, map_, default_] := Replace[key, Append[map, _ -> default]]

run[args_] := Module[{args2=Rule@@@Partition[Rest[args],2]},
  AblationStudyReport[replaceWithDefault["-i", args2, $Failed],
    replaceWithDefault["-html", args2, $Failed],
    replaceWithDefault["-csv", args2, $Failed]
  ];
]

run[$ScriptCommandLine];

ResetDirectory[]
