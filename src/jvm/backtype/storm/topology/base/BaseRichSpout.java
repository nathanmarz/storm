/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package backtype.storm.topology.base;

import backtype.storm.topology.IRichSpout;

/**
 *
 * @author nathan
 */
public abstract class BaseRichSpout extends BaseComponent implements IRichSpout {
    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
}
