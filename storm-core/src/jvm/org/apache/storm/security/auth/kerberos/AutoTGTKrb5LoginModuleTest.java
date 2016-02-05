/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.security.auth.kerberos;

import java.security.Principal;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * Custom LoginModule extended for testing.
 */
public class AutoTGTKrb5LoginModuleTest extends AutoTGTKrb5LoginModule {

    public Principal client = null;

    public void setKerbTicket(KerberosTicket ticket) {
        this.kerbTicket = ticket;
    }
    
    @Override
    protected void getKerbTicketFromCache() {
        // Do nothing.
    }

    @Override
    protected Principal getKerbTicketClient() {
        return this.client;
    }
}
