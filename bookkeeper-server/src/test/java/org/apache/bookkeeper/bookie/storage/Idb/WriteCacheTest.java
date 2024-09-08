package org.apache.bookkeeper.bookie.storage.Idb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteCacheTest {

    //--------------Casi di test per il metodo Put ricavati tramite la Boundary Value Analysis-------------------------------
    @Test
    public void testPutCacheNotFull() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 10 * 1024);
        ByteBuf entry = Unpooled.buffer(1024);
        boolean actual = wc.put(0,0, entry);
        assertTrue(actual);
    }

    @Test
    public void testPutCacheFull() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 1024);
        ByteBuf entry = Unpooled.buffer(1024);
        entry.writerIndex(entry.capacity());
        wc.put(0,0, entry);
        boolean actual2 = wc.put(0, 0, entry);
        assertFalse(actual2);
    }

    @Test
    public void testPutNull() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 10 * 1024);
        ByteBuf entry = null;
        assertThrows(NullPointerException.class,()->wc.put(0, 1, entry));
    }

    @Test
    public void testInvalidLedgerId() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 10 * 1024);
        ByteBuf entry = Unpooled.buffer(1024);
        assertThrows(IllegalArgumentException.class,()->wc.put(-1, 0, entry));
    }

    @Test
    public void testInvalidEntryId() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 10 * 1024);
        ByteBuf entry = Unpooled.buffer(1024);
        assertThrows(IllegalArgumentException.class,()->wc.put(0, -1, entry));
    }

    private ByteBuf getMockedInvalidEntry() {
        ByteBuf invalidEntry = mock(ByteBuf.class);
        when(invalidEntry.readableBytes()).thenReturn(1);
        when(invalidEntry.readerIndex()).thenReturn(-1);
        return invalidEntry;
    }

    @Test
    public void testInvalidEntry() {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 10 * 1024);
        ByteBuf entry = getMockedInvalidEntry();
        assertThrows(IndexOutOfBoundsException.class,()->wc.put(0, 0, entry));
    }

    //----------------Casi di test per migliorare Mutation Coverage del metodo put-----------------------

    @Test
    public void newCacheFull() throws Exception {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        WriteCache wc = new WriteCache(allocator, 5*1024);
        ByteBuf entry = allocator.buffer(1024);
        entry.writerIndex(entry.capacity());
        for (int i = 0; i < 5; i++) {
            assertTrue(wc.put(0, i, entry));
        }
        assertFalse(wc.put(0, 6, entry));
        assertEquals(5, wc.count());
    }

}
