package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferedChannelTest {

    //--------------Casi di test per il metodo Write ricavati tramite la Boundary Value Analysis-------------------------------
    @Test
    public void testValidWrite() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
        String msg = "Hello world!";
        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 65536, 512, 30);
        ByteBuf src = Unpooled.buffer(100).writeBytes(msg.getBytes());
        bf.write(src);
        assertEquals(msg.length(), bf.getUnpersistedBytes());
    }

    @Test
    public void testValidWrite2() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
        String msg = "Hello world!";
        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 65536, 512, 2);
        ByteBuf src = Unpooled.buffer(100).writeBytes(msg.getBytes());
        bf.write(src);
        assertEquals(0, bf.getUnpersistedBytes());
    }

    private ByteBuf getMockedInvalidSrc() {
        ByteBuf invalidSrc = mock(ByteBuf.class);
        when(invalidSrc.readableBytes()).thenReturn(1);
        when(invalidSrc.readerIndex()).thenReturn(-1);
        return invalidSrc;
    }

    @Test
    public void testInvalidWriteMockito() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
        String msg = "Hello world!";
        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 65536, 512, 2);
        ByteBuf invalidSrc = getMockedInvalidSrc();
        assertThrows(IndexOutOfBoundsException.class,()->bf.write(invalidSrc));

    }

    @Test
    public void testWriteWithSrcNull() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();

        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 65536, 512, 30);
        ByteBuf src = null;
        assertThrows(NullPointerException.class,()->bf.write(src));
    }

    //----------------Casi di test per migliorare Mutation Coverage e Data Flow Coverage del metodo Write-----------------------

    @Test
    public void newTestWrite1() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
        String msg = "Hello world!";
        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 65536, 10, 30);
        ByteBuf src = Unpooled.buffer(100).writeBytes(msg.getBytes());
        bf.write(src);
        assertEquals(msg.length(), bf.getUnpersistedBytes());
    }

    @Test
    public void newTestWrite2() throws IOException {
        File tempFile = File.createTempFile("test", "log");
        tempFile.deleteOnExit();
        FileChannel fc = new RandomAccessFile(tempFile, "rw").getChannel();
        String msg = "Hello world!";
        BufferedChannel bf = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, 5, 512, 30);
        ByteBuf src = Unpooled.buffer(100).writeBytes(msg.getBytes());
        bf.write(src);
        assertEquals(msg.length(), bf.getUnpersistedBytes());
    }


}
