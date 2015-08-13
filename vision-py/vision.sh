scripts=./vision-py/scripts
pngDir=/Users/rodneykinney/Downloads/RegentsRun/regentsImagesResized
outputDir=./vision-out

python $scripts/ExtractArrows.py -i $pngDir -o $outputDir/arrows

python $scripts/ExtractBlobs.py -i $pngDir -o $outputDir/blobs

python $scripts/ExtractText.py -i $pngDir -o $outputDir/text

python $scripts/ExtractRelations.py -a $outputDir/arrows -b $outputDir/blobs -t $outputDir/text -o $outputDir/relations