package org.example;

import com.yahoo.container.jdisc.HttpResponse;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CustomHttpResponse extends HttpResponse {

    private final byte[] data;

    public CustomHttpResponse(int status, byte[] data) {
        super(status);
        this.data = data;
    }

    @Override
    public void render(OutputStream outputStream) throws IOException {
        outputStream.write(data);
    }

    @Override
    public String getContentType() {
        return "application/json";
    }
}