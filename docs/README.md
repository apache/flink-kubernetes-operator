This README gives an overview of how to build and contribute to the
documentation of the Flink Kubernetes Operator.

The documentation is included with the source of the Flink Kubernetes Operator in order to ensure
that you always have docs corresponding to your checked-out version.

# Requirements

### Generate the java doc (only if anything changed)
```sh
mvn clean install -DskipTests -Pgenerate-docs
```

### Build the site locally

#### Using Hugo Docker image:

```sh
$ git submodule update --init --recursive
$ docker run -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest server --buildDrafts --buildFuture --bind 0.0.0.0
```

#### Local Hugo installation:

Make sure you have installed [Hugo](https://gohugo.io/getting-started/installing/) on your system.

```sh
$ git submodule update --init --recursive
$ hugo -b "" serve
```

The site can be viewed at http://localhost:1313/

# Contribute

## Markdown

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible
to use [GitHub flavored
syntax](http://github.github.com/github-flavored-markdown) and intermix plain
html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which
specifies the title of the page and the layout to use. The title is used as the
top-level heading for the page.

    ---
    title: "Title of the Page"
    ---

    ---
    title: "Title of the Page" <-- Title rendered in the side nav
    weight: 1 <-- Weight controls the ordering of pages in the side nav
    type: docs <-- required
    aliases:  <-- Alias to setup redirect from removed page to this one
      - /alias/to/removed/page.html
    ---

## Structure

### Page

#### Headings

All documents are structured with headings. From these headings, you can
automatically generate a page table of contents (see below).

```
# Level-1 Heading  <- Used for the title of the page 
## Level-2 Heading <- Start with this one for content
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with
level-2 headings and use level-3 headings for subsections, etc. Don't use a
different ordering, because you don't like how a headline looks.

#### Table of Contents

Table of contents are added automatically to every page, based on heading levels
2 - 4. The ToC can be omitted by adding the following to the front matter of
the page:

    ---
    bookToc: false
    ---

### ShortCodes 

Flink uses [shortcodes](https://gohugo.io/content-management/shortcodes/) to add
custom functionality to its documentation markdown. For example:

    {{< artifact flink-streaming-java withScalaVersion >}}

This will be replaced by the maven artifact for flink-streaming-java that users
should copy into their pom.xml file. It will render out to:

```xml
<dependency>
    <groupdId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version><!-- current flink version --></version>
</dependency>
```

It includes a number of optional flags:

* withScalaVersion: Includes the scala version suffix to the artifact id
* withTestScope: Includes `<scope>test</scope>` to the module. Useful for
  marking test dependencies.
* withTestClassifier: Includes `<classifier>tests</classifier>`. Useful when
  users should be pulling in Flinks tests dependencies. This is mostly for the
  test harnesses and probably not what you want. 

Its implementation and documentation can be found at
`./layouts/shortcodes/artifact.html`. Please refer to `./layouts/shortcodes/`
for other shortcodes available.
