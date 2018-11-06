package geotrellis.contrib.vlm.gdal

import org.gdal.gdal.Dataset

import java.io.Closeable

/** GDAL Datasets wrapper to make GC happy */
case class GDALDataset(dataset: Dataset) extends Closeable{
  val underlying: Dataset = dataset

  def close: Unit = dataset.delete()

  override def finalize(): Unit = {
    if(dataset != null) close
    super.finalize()
  }
}
