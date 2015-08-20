colorToHTML[c_]:=With[{clist=List@@ColorConvert[c,"RGB"]},"#"<>ToUpperCase[IntegerString[Flatten[IntegerDigits[#,16,2]&/@Round[clist[[1;;3]]*255]],16]]]


styleToHTML[data_,FontColor->c_]:=XMLElement["span",{"style"->"color:"<>colorToHTML[c]},Flatten[{data}]]


styleToHTML[data_,FontWeight->Bold]:=XMLElement["b",{},Flatten[{data}]]


styleToHTML[data_,FontSlant->Italic]:=XMLElement["i",{},Flatten[{data}]]


styleToHTML[data_,FontSize->num_]:=XMLElement["span",{"style"->"font-size:"<>ToString[num]<>"px"},Flatten[{data}]]


styleToHTML[data_,n_]:=(Print["Unknown style option: ",n];)


processStyleOption[x:(_RGBColor|_Hue|_CMYKColor|_GrayColor)]:=FontColor->x
processStyleOption[x:(Bold|Plain)]:=FontWeight->x
processStyleOption[x:Italic]:=FontSlant->Italic
processStyleOption[n:(_Real|_Integer)]:=FontSize->n
processStyleOption[x_]:=x


processGrid[Grid[x_,opts___]]:=Module[{elem,bg},XMLElement["table",{"style"->"border-collapse:collapse"},XMLElement["tr",{},(bg=None;elem=Replace[#,Item[e_,Background->bg1_]:>(bg=bg1;e)];XMLElement["td",{"style"->"border: 1px solid black"<>If[bg=!=None,"; background-color:"<>colorToHTML[bg],""]},Flatten[{elem}/.$exprToHTMLRules]])&/@#]&/@x]]


$exprToHTMLRules:={Row[x_]:>(x/.$exprToHTMLRules),Row[x_,y_]:>Flatten[Riffle[x/.$exprToHTMLRules,y]],n_Integer|n_Real:>ToString[n],
g:Grid[x_,opts___]:>processGrid[g],
(Item|Style)[x_,opts___]:>Fold[styleToHTML,x/.$exprToHTMLRules,processStyleOption/@Flatten[{opts}]],s_String:>(StringReplace[s,"\n"->XMLElement["br",{},{}]]/.StringExpression->List)}


exprToHTML[data_]:=Module[{},Flatten[data/.$exprToHTMLRules]]


showExprToHTML[data_]:=SystemOpen[Export["tmp.html",XMLElement["html",{},{XMLElement["div",{"style"->"font-size:14px; font-family:Arial"},Flatten[{exprToHTML[data]}]]}],"XML"]]



