<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'
                xmlns="http://www.w3.org/TR/xhtml1/transitional"
                exclude-result-prefixes="#default">

<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/xhtml/docbook.xsl"/>

<xsl:param name="html.stylesheet" select="'stylesheet.css'"></xsl:param>

<xsl:param name="toc.max.depth" select="4"></xsl:param>
<xsl:param name="toc.section.depth" select="3"></xsl:param>

<xsl:template match="refentry" mode="toc">
  <xsl:apply-templates mode="toc" select="refsect1" />
</xsl:template>

<xsl:template match="refsect1" mode="toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="refsect2"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="refsect2" mode="toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
    <xsl:with-param name="nodes" select="refsect3"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="refsect3" mode="toc">
  <xsl:param name="toc-context" select="."/>
  <xsl:call-template name="subtoc">
    <xsl:with-param name="toc-context" select="$toc-context"/>
  </xsl:call-template>
</xsl:template>


<xsl:template match="productname">
  <xsl:call-template name="inline.charseq"/>
</xsl:template>

<xsl:param name="generate.toc">
book/reference toc, title
</xsl:param>

</xsl:stylesheet>
