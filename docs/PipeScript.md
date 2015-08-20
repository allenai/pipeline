PipeScript
===

PipeScript is a simple scripting language that makes your workflows reproducible and sharable.

A PipeScript file can execute an arbitrary sequence of commands. After executing them, it will

- Upload all inputs to a shared location
- Write all outputs to a shared location
- Create a portable script that any user can run from anywhere to exactly reproduce the results
- Create an HTML page with a visualization of the workflow, with links to all input, intermediate, and final data 

Suppose you have a shell script named `ProcessData.sh` that takes an input file (specified
with an `--input` argument) and produces an output file (specified with an `--output` argument).
The equivalent PipeScript file would be:
 
    run {input: ProcessData.sh} --input {input: InputData.txt} --output {output: result}
     
When PipeScript executes this script, it will upload each input to the output location, using
a file name that includes a checksum of the file contents. Later, if you change 
the contents of `ProcessData.sh` or `InputData.txt` and rerun the script, new copies of the inputs will be upload, but
the old versions will still be retained. Thus, a complete history of all past PipeScript run
is kept in the output location, allowing any past result to be reproduced.

You can run a PipeScript file with  

    pipeScript/bin/runPipeScript <PipeScript-file> <output-location-url>
    
where `output-location-url` can be a `file:` or `s3:` URL.  If you use `s3:` URLs, you must set the
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY` environment variables.  

The HTML page summarizing the script
execution will also contain a link to a portable script file, in which the local file paths have been
replaced with corresponding urls in the output location, e.g.

    run {input: "<output-location-url>/ProcessData.<hash-code>.sh"} 
       --input {input: "<output-location-url>/InputData.<hash-code>.txt"} 
       --output {output: result}
 
The portable script can be run from any machine and will produce the same results as the original run.

# Language Definition

PipeScript is a simple language. At its core is the `run` command, which specifies a system process
and its arguments. Each argument is either a literal string or a *resource* (i.e. an input/output file
or directory)

## Comments
Use the `#` character to ignore the remainder of a line

## Strings
Quotes are optional in a string if it does not contain whitespace or ```{}`:,```. If quoted, 
it can use escape characters in the same way as Java. Strings of the form `s"..."` (quoted with an `s`
character before the beginning quote) will perform variable substitution on their contents (similar to Scala)

## Variables
Variables are set with the `set` command:

    set {var1: value1, var2: "http://www.example.com/value2"}
    
Variables can be used after they are declared by using a `$` prefix

    $var1                     # => value1
    ${var1}                   # => value1
    s"${var1}23"              # => value123
    s"url=\"${var2}\""        # => url="http://www.example.com/value2"
    
## Packages
A *package* is a directory of files. It will be uploaded to the output location. Files within the
package may be used by `run` commands by specifying a *file* resource. The most common use is for directories containing script files.

    package {id:<package-id>, source:<source-url>}

where *package-id* is a unique name for the package and *source-url* is a file path to a local directory or `s3:`
URL pointing to a zip file (which will be downloaded and expanded).

## Run Command
The `run` command indicates a system process to execute.

    run <args>

where each argument is either a string (as defined above) or a *resource*. A *resource* is specified 
by `key:value` pairs separated by commas within curly braces. The different kinds of resources are:

#### `{input:<source-url>, type:<file|dir>}` 
An input file or directory, where *source-url* is a local file path or `s3:` URL.
At runtime, this will resolve to an absolute filesystem path.  If not
specified, the default type is `file`.

####`{file:<file-name>,  package:<package-id>}` 
An individual file within a *package* declared earlier

####`{output:<output-id>, type:<file|dir>, suffix:<suffix>`}
An output file or directory. At runtime, this will resolve to an absolute filesystem path. 
**The system process must create a file/directory at that location when it finishes**. If not
specified, the default type is `file`. The output will be uploaded with a name of the form `<output-id>.<hash-code><suffix>`.
 
####`{ref:<output-id>}`
The output of a previous `run` command, matched by *output-id*. At runtime, this will resolve
to an absolute local filesystem path.

# Example
Here is an example of a prototypical Python-based workflow

    set {imageDir: /home/ps-user/data/images, scriptDir: /home/ps-user/python/classification-scripts}
    
    package {id:scripts, source: $scriptDir}
    
    run python {file:TrainTestSplit.py, package: scripts} 
        -input $imageDir 
        -outTrain {output:trainImages, type:dir} 
        -outTest {output:testImages, type:dir}
        
    run python {file:TrainModel.py, package: scripts}
        -images {ref: trainImages}
        -output {output:modelFile}
        
     run python {file: EvalModel.py, package: scripts}
         -images {ref:testImages}
         -model {ref:modelFile}
         -output {output: evaluation, suffix:".txt"}
