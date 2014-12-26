#Storm Redis

Storm/Trident integration for [Redis](http://redis.io/)

## Usage

### How do I use it?

You can download the latest build at:
    http://github.com/dashengju/storm-redis/releases

Or use it as a maven dependency:

```xml
<repositories>
    <repository>
        <id>storm-redis</id>
        <url>https://raw.github.com/dashengju/storm-redis/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>
```

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-redis</artifactId>
    <version>1.0.1</version>
    <type>jar</type>
    <scope>compile</scope>
</dependency>
```

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Committer Sponsors

 * DashengJu ([dashengju@gmail.com](mailto:dashengju@gmail.com))
 
