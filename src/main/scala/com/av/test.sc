trait AbstractOperation {
  def run()
}

class SaveToDatabaseOperation extends AbstractOperation {
  override def run(): Unit = println("Save to db")
}

trait AuditDecorator extends AbstractOperation {
  abstract override def run(): Unit = {
    println("Entering Audit")
    super.run()
    println("Exiting Audit")
  }
}

trait CachingDecorator extends AbstractOperation {
  abstract override def run(): Unit = {
    println("Entering Cache")
    super.run()
    println("Exiting Cache")
  }
}

val operation = new SaveToDatabaseOperation with AuditDecorator with CachingDecorator
operation.run()



