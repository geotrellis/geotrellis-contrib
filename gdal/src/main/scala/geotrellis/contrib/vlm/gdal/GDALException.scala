package geotrellis.contrib.vlm.gdal


/** Base class for all Exceptions involving GDAL. */
class GDALException(message: String) extends Exception(message)

/** Exception thrown when data coulld not be read from data source. */
class GDALIOException(message: String) extends Exception(message)

/** Exception thrown when the attributes of a data source are found to be bad. */
class MalformedDataException(message: String) extends GDALException(message)

/** Exception thrown when the DataType of a data source is found to be bad. */
class MalformedDataTypeException(message: String) extends GDALException(message)

/** Exception thrown when the projection of a data source is found to be bad. */
class MalformedProjectionException(message: String) extends GDALException(message)
