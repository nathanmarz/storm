# Apache Storm Website and Documentation
This is the source for the Release specific part of the Apache Storm website and documentation. It is statically generated using [jekyll](http://jekyllrb.com).

## Generate Javadoc

You have to generate javadoc on project root before generating document site.

```
mvn javadoc:javadoc
mvn javadoc:aggregate -DreportOutputDirectory=./docs/ -DdestDir=javadocs
```

You need to create distribution package with gpg certificate. Please refer [here](https://github.com/apache/storm/blob/master/DEVELOPER.md#packaging).

## Site Generation
First install jekyll (assuming you have ruby installed):

```
gem install jekyll
```

Generate the site, and start a server locally:
```
cd docs
jekyll serve -w
```

The `-w` option tells jekyll to watch for changes to files and regenerate the site automatically when any content changes.

Point your browser to http://localhost:4000

By default, jekyll will generate the site in a `_site` directory.

This will only show the portion of the documentation that is specific to this release.

## Adding a new release to the website
In order to add a new relase, you must have committer access to Storm's subversion repository at https://svn.apache.org/repos/asf/storm/site.

Release documentation is placed under the releases directory named after the release version.  Most metadata about the release will be generated automatically from the name using a jekyll plugin.  Or by plaing them in the _data/releases.yml file.

To create a new release run the following from the main git directory

```
mvn javadoc:javadoc
mvn javadoc:aggregate -DreportOutputDirectory=./docs/ -DdestDir=javadocs
cd docs
mkdir ${path_to_svn}/releases/${release_name}
cp -r *.md images/ javadocs/ ${path_to_svn}/releases/${release_name}
cd ${path_to_svn}
svn add releases/${release_name}
svn commit
```

to publish a new release run

```
cd ${path_to_svn}
jekyll build -d publish/
svn add publish/ #Add any new files
svn commit
```
