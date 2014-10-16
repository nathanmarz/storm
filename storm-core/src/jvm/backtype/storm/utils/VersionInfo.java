package backtype.storm.utils;

public class VersionInfo {
  private static Package myPackage;
  private static VersionAnnotation version;

  static {
    myPackage = VersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(VersionAnnotation.class);
  }
  
  static Package getPackage() {
    return myPackage;
  }

  public static String getVersion() {
    return version != null ? version.version() : "Unknown";
  }

  public static String getRevision() {
    return version != null ? version.revision() : "Unknown";
  }

  public static String getBranch() {
    return version != null ? version.branch() : "Unknown";
  }

  public static String getDate() {
    return version != null ? version.date() : "Unknown";
  }

  public static String getUser() {
    return version != null ? version.user() : "Unknown";
  }

  public static String getUrl() {
    return version != null ? version.url() : "Unknown";
  }

  public static String getSrcChecksum() {
    return version != null ? version.srcChecksum() : "Unknown";
  }

  public static String getBuildVersion() {
    return VersionInfo.getVersion() + " from " + VersionInfo.getRevision()
        + " by " + VersionInfo.getUser() + " source checksum "
        + VersionInfo.getSrcChecksum();
  }

  public static void main(String[] args) {
    System.out.println("Storm " + getVersion());
    System.out.println("Subversion " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }

}
