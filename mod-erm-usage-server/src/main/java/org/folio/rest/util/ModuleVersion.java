package org.folio.rest.util;

import java.io.FileReader;
import java.io.IOException;
import org.apache.maven.model.Parent;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

public class ModuleVersion {

  private static String version;

  private ModuleVersion() {}

  public static String getModuleVersion() throws IOException, XmlPullParserException {
    if (version == null) {
      Parent parent = new MavenXpp3Reader().read(new FileReader("pom.xml")).getParent();
      version = String.format("%s-%s", parent.getArtifactId(), parent.getVersion());
    }
    return version;
  }
}
