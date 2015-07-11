How to implement a Producer
=====

A Producer is conceptually a function that is decorated with enough metadata to build a Signature. Recall
that a Signature is used to determine a unique path name where the output of this Producer will be written.
The easiest way to define a Producer class is to make a case class that mixes in the Ai2StepInfo trait.
The only method that needs to be implemented in that case is the `create` method, which builds the output object
For example:

    case class CountLines(lines: Producer[Iterable[String]], countBlanks: Boolean = true) extends Producer[Int] with Ai2StepInfo {
      override protected def create: Int =
      if (countBlanks)
        lines.get.size
      else
        lines.get.filter(_.length > 0).size
    }

Notice how each Producer's `create` method calls the `get` method of its inputs. (`get` is simply an in-memory cache of
the result of `create`)  This is the mechanism by
which the end-to-end workflow is executed: the `Pipeline.run` method calls `get` on each persisted Producer.
The workflow graph is isomorphic to the object graph of Producers with references to other Producers.

The Signature of this Producer depends on the value of the `countBlanks` parameter, but also on the Signature of its
input, the Producer[Iterable[String]] whose lines it is counting.  That Producer's Signature depends likewise on
its own parameters and inputs, etc.  The outcome is that this Producer's output will be written to a
different location depending on where in a workflow it it plugged in.

Occasionally, it is necessary to change the logic of a Producer, such that its behavior will be different
from previous versions of the code.  The Signature includes a class-version field for this purpose. To indicate a change in the logic of a Producer, override the
`versionHistory` method.  For example:

    case class CountLines(lines: Producer[Iterable[String]], countBlanks: Boolean = true) extends Producer[Int] with Ai2StepInfo {
      override protected def create: Int =
        if (countBlanks)
          lines.get.size
        else
          lines.get.filter(_.trim.length > 0).size

      override def versionHistory = List(
        "v1.1" // Count whitespace-only lines as blank
      )
    }

In this way, cached data produced by older versions of your code can coexist with more recent versions.  Different
users can share data without conflict despite possibly running different versions of the code. (The value of the
version field can be any string, so long as it is unique.)

What happens if you change the logic of a Producer but forget to update the `versionHistory` method?
Even in this case, it is impossible to overwrite existing data.  Instead, your Producer may end up reading cached
data instead of recomputing based on the new logic.  To force a recomputation, you must change the Signature by updating the
`versionHistory` field.

