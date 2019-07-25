# Releasing GeoTrellis Contrib

Ensure you have bintray credentials set: https://github.com/sbt/sbt-bintray#credentials

Ensure `master` is up to date.

Create a new release branch `git checkout -b release/x.y.z`

Bump each `version.sbt` to match the version you're releasing.

Update the CHANGELOG. Ensure you've included a new link to the current release at the
bottom of the file and kept the `[Unreleased]` header at the top.

Commit all your changes.

Time to release!
```bash
./sbt publish
./sbt -212 publish
./sbt bintrayRelease
```

Tag the release with `v<x.y.z`, e.g. `v3.17.0` and push the new tag.

Bump each `version.sbt` to x.y.(z + 1)-SNAPSHOT, e.g. if previous release was 
`3.17.0`, use `3.17.1-SNAPSHOT`. Commit. If a more significant version change is required,
it can be handled during the next release.

Merge `release/x.y.z` back into `master`. 

