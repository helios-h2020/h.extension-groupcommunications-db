package eu.h2020.helios_social.modules.groupcommunications.db.data;

import eu.h2020.helios_social.modules.groupcommunications_utils.Bytes;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfDictionary;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfList;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfWriter;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfWriterFactory;
import eu.h2020.helios_social.modules.groupcommunications_utils.data.Encoder;
import eu.h2020.helios_social.modules.groupcommunications_utils.db.Metadata;
import eu.h2020.helios_social.modules.groupcommunications_utils.nullsafety.NotNullByDefault;
import eu.h2020.helios_social.modules.groupcommunications.api.exception.FormatException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import static eu.h2020.helios_social.modules.groupcommunications_utils.data.BdfDictionary.NULL_VALUE;
import static eu.h2020.helios_social.modules.groupcommunications_utils.db.Metadata.REMOVE;
import static java.util.logging.Logger.getLogger;

@Immutable
@NotNullByDefault
class EncoderImpl implements Encoder {
    private static Logger LOG =
            getLogger(Encoder.class.getName());

    private final BdfWriterFactory bdfWriterFactory;

    @Inject
    EncoderImpl(BdfWriterFactory bdfWriterFactory) {
        this.bdfWriterFactory = bdfWriterFactory;
    }

    @Override
    public Metadata encodeMetadata(BdfDictionary d) throws FormatException {
        Metadata m = new Metadata();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BdfWriter writer = bdfWriterFactory.createWriter(out);
        try {
            for (Entry<String, Object> e : d.entrySet()) {
                if (e.getValue() == NULL_VALUE) {
                    // Special case: if value is null, key is being removed
                    m.put(e.getKey(), REMOVE);
                } else {
                    encodeObject(writer, e.getValue());
                    m.put(e.getKey(), out.toByteArray());
                    out.reset();
                }
            }
        } catch (FormatException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return m;
    }

    @Override
    public byte[] encodeToByteArray(BdfList list) throws FormatException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BdfWriter writer = bdfWriterFactory.createWriter(out);
        try {
            writer.writeList(list);
        } catch (FormatException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    private void encodeObject(BdfWriter writer, Object o)
            throws IOException {
        if (o instanceof Boolean) writer.writeBoolean((Boolean) o);
        else if (o instanceof Byte) writer.writeInteger((Byte) o);
        else if (o instanceof Short) writer.writeInteger((Short) o);
        else if (o instanceof Integer) writer.writeInteger((Integer) o);
        else if (o instanceof Long) writer.writeLong((Long) o);
        else if (o instanceof Float) writer.writeDouble((Float) o);
        else if (o instanceof Double) writer.writeDouble((Double) o);
        else if (o instanceof String) writer.writeString((String) o);
        else if (o instanceof byte[]) writer.writeRaw((byte[]) o);
        else if (o instanceof Bytes) writer.writeRaw(((Bytes) o).getBytes());
        else if (o instanceof List) {
            writer.writeList((List) o);
        } else if (o instanceof Map) {
            writer.writeDictionary((Map) o);
        } else throw new FormatException();
    }
}
