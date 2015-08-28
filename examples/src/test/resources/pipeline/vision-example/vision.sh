scripts=./src/test/resources/pipeline/vision-example/scripts
pngDir=./src/test/resources/pipeline/vision-example/png
outputDir=./vision-example-out

python $scripts/ExtractArrows.py -i $pngDir -o $outputDir/arrows

python $scripts/ExtractBlobs.py -i $pngDir -o $outputDir/blobs

python $scripts/ExtractText.py -i $pngDir -o $outputDir/text

python $scripts/ExtractRelations.py -i $pngDir -a $outputDir/arrows -b $outputDir/blobs -t $outputDir/text -o $outputDir/relations