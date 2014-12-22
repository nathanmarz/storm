# Apache Storm Website and Documentation
This is the source for the Storm website and documentation. It is statically generated using [jekyll](http://jekyllrb.com).


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


## Publishing the Website
In order to publish the website, you must have committer access to Storm's subversion repository.

The Storm website is published using Apache svnpubsub. Any changes committed to subversion will be automatically published to storm.apache.org.

To publish changes, tell jekyll to generate the site in the `publish` directory of subversion, then commit the changes:


```
cd docs
jekyll build -d /path/to/svn/repo/publish
cd /path/to/svn/repo/publish
svn commit
```
