
object HdfsApp extends App {
  HdfsService.compactDirectory("/stage", "/ods")
}
