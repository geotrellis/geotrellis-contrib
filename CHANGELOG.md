# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Receive GPG key while publishing artifacts [#243](https://github.com/geotrellis/geotrellis-contrib/pull/243)

### Changed

- Artifacts are published to https://oss.sonatype.org automatically via CircleCI
  - The `bintray` resolver in build.sbt is no longer necessary as of this release
  - Commits to the develop branch are available as SNAPSHOT releases
- Move RasterSources into GeoTrellis core, only RasterSources effects are present in this repository now.

## [3.17.1] - 2019-08-26

### Added

- Expose GDAL Metadata API, expose RasterSources Metadata API
- LayoutTileSource[K] implementation
- Add Named RasterSources
- Implement interpretAs on RasterSources

### Fixed

- In RasterSource, match on TargetCellType instead of ConvertTargetCellType to prevent always falling through
- RasterSource is now java serializable

## [3.17.0] - 2019-07-23

### Added

- RasterSourceF experimental implementation

### Fixed

- VLM: `GeoTiffRasterSource` reads are now thread safe
- VLM: `GeoTiffRasterSource`s will now reuse a tiff instead of rereading
  it when possible.

## [3.16.0] - 2019-07-08

### Added

- VLM: Created the `DataPath` type which the `RasterSource`s now take
- VLM: Created an SPI interface for `RasterSource`
- VLM: Use scala-uri library for URI parsing
- GDAL: `GDALException` and its subclasses

### Fixed

- VLM: Correct incomplete reads when using `MosaicRasterSource`
- VLM: `RasterSource` will now short circut reprojection if the given
  source is already in the target CRS.
- RasterSourceF experimental implementation
- GDAL: `GDALDataset` will now throw more helpful error messages
- GDALRasterSourceF experimental implementation

### Removed

- Summary: Subproject removed. The polygonal summary prototype was moved to GeoTrellis core for the 3.0 release. See: https://github.com/locationtech/geotrellis/blob/master/docs/guide/rasters.rst#polygonal-summary

### Fixed

- VLM: RasterSourceRDD.read and RasterSourceRDD.tileLayerRDD partition count

## [3.15.0] - 2019-06-22

### Added

- Fix GDAL resampling with Dimensions resampleGrid
- CHANGELOG based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
- VLM: Add support for ByteReaders backed by HDFS

## [3.14.0] - 2019-05-17

### Added

- VLM: RasterSourceRDD can accept optional RasterSummary

### Changed

- GDAL: GDALWarpOptions.convert is now consistent with reproject and resample operations
- Slick: Do not publish to bintray

### Fixed

- GDAL: Improve GDALWarpOptions.convert behavior
- GDAL: GDALRasterSource reports different extent from GeoTiffRasterSource
- VLM: MosaicRasterSource slow metadata fetches for large mosaics
- VLM: Combine raster extents with round instead of ceil
- VLM: RasterSummary.fromSeq improperly constructs CellSize

### Removed

- GDAL: GDALReprojectRasterSource - this operation is now available as GDALRasterSource.reproject
- GDAL: GDALResampleRasterSource - this operation is now available as GDALRasterSource.resample

## [3.13.0] - 2019-05-17

### Added

- Slick: Project moved to geotrellis-contrib from [geotrellis](https://github.com/locationtech/geotrellis)
- VLM: GeotrellisRasterSource.sparseStitch to infer missing tiles

### Changed

- Bump to geotrellis 3.0.0-M3

### Fixed

- GDAL: Spelng mstke in GDALWarp exception message

## [3.11.0] - 2019-04-08

### Added

- GDAL: Run tests in CI
- GDAL: GDALDataset: Replaces use of GDALWarp.get_token

### Changed

- Upgrade geotrellis 2.x -> 3.x

### Fixed

- GDAL: Tests do not pass

[unreleased]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.17.1...HEAD
[3.17.1]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.17.0...v3.17.1
[3.17.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.16.0...v3.17.0
[3.16.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.15.0...v3.16.0
[3.15.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.14.0...v3.15.0
[3.14.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.13.0...v3.14.0
[3.13.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v3.11.0...v3.13.0
[3.11.0]: https://github.com/geotrellis/geotrellis-contrib/compare/v0.11.0...v3.11.0
