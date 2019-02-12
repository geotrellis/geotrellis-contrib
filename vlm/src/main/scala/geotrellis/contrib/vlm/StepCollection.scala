package geotrellis.contrib.vlm


case class StepCollection(steps: List[Step] = List()) {
  def update(step: Step): StepCollection =
    steps.lastOption match {
      case Some(lastStep) if step.replacePrevious(lastStep) => StepCollection(steps.slice(0, steps.size - 1) :+ step)
      case Some(lastStep) => StepCollection(steps :+ step)
      case None => StepCollection(List(step))
    }

  def status: String =
    if (steps.isEmpty)
      "No additional steps have been taken"
    else
    "These are the steps that will be taken:\n" + steps.map { _.message }.reduce { _ + "\n" + _ }
}
