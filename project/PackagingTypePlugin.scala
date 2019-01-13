import sbt._

// Workaround for known issue with importing some libraries with SBT
// see https://github.com/sbt/sbt/issues/3618#issuecomment-424924293
object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}