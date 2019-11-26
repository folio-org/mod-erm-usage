package org.folio.rest.util;

import org.apache.maven.model.Parent;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.FileReader;
import java.io.IOException;

public class ModuleVersion {

  private static String version;

  private ModuleVersion() {
    throw new IllegalStateException("Utility class");
  }

  public static String getModuleVersion() throws IOException, XmlPullParserException {
    if (version == null) {
      Parent parent = new MavenXpp3Reader().read(new FileReader("pom.xml")).getParent();
      version = String.format("%s-%s", parent.getArtifactId(), parent.getVersion());
    }
    return version;
  }
}
