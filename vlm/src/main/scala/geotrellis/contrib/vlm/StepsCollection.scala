package geotrellis.contrib.vlm


case class StepsCollection(steps: List[Step] = List()) {
  def update(step: Step): StepsCollection =
    steps.lastOption match {
      case Some(lastStep) if step.replacePrevious(lastStep) => StepsCollection(steps.slice(0, steps.size - 1) :+ step)
      case Some(lastStep) => StepsCollection(steps :+ step)
      case None => StepsCollection(List(step))
    }

  def status: String =
    if (steps.isEmpty)
      "No additional steps have been taken"
    else
    "These are the steps that will be taken:\n" + steps.map { _.message }.reduce { _ + "\n" + _ }
}
