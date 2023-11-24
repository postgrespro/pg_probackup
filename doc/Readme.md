# Generating documentation
```
xmllint --noout --valid probackup.xml
xsltproc stylesheet.xsl probackup.xml >pg-probackup.html
```
> [!NOTE]
>Install ```docbook-xsl``` if you got
>``` "xsl:import : unable to load http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"``` 