import { auth } from "@clerk/nextjs/server";
import { generateGeminiContentStream } from "@/lib/gemini";
import { db } from "@/lib/prisma";

export async function POST(request) {
  const { userId } = await auth();

  if (!userId) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  const apiKey = process.env.GEMINI_API_KEY;

  if (!apiKey) {
    return new Response(
      JSON.stringify({ error: "GEMINI_API_KEY is not configured" }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  let prompt;
  let conversationId;

  try {
    const body = await request.json();
    prompt = body.prompt;
    conversationId = body.conversationId;
  } catch {
    return new Response(
      JSON.stringify({ error: "Invalid request body" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    return new Response(
      JSON.stringify({ error: "Prompt is required" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  const user = await db.user.findUnique({
    where: {
      clerkUserId: userId,
    },
  });

  if (!user) {
    return new Response(JSON.stringify({ error: "User not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (conversationId) {
    const conversation = await db.conversation.findFirst({
      where: {
        id: conversationId,
        userId: user.id,
      },
    });

    if (!conversation) {
      return new Response(
        JSON.stringify({ error: "Conversation not found" }),
        {
          status: 404,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    await db.message.create({
      data: {
        conversationId,
        role: "user",
        content: prompt,
      },
    });

    await db.conversation.update({
      where: {
        id: conversationId,
      },
      data: {
        updatedAt: new Date(),
      },
    });
  }

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      let fullResponse = "";

      try {
      const restrictedPrompt = `
You are Pathfinder AI, a professional career guidance assistant.

Your scope includes ALL professional and career-related domains, including:
- software engineering
- medicine
- healthcare
- law
- finance
- accounting
- banking
- business
- management
- marketing
- sales
- design
- UI/UX
- architecture
- education
- teaching
- research
- government jobs
- civil services
- entrepreneurship
- freelancing
- consulting
- skilled trades
- manufacturing
- logistics
- human resources
- customer support
- media
- content creation
- non-technical professions

You help users with:
- career guidance
- interview preparation
- mock interviews
- resume/CV improvement
- cover letters
- job applications
- job search strategy
- skill development
- certification guidance
- learning roadmaps
- salary discussions
- career transitions
- workplace growth
- professional development

Rules:
- Stay focused on careers and professional growth.
- If the user asks something completely unrelated (jokes, entertainment, random trivia, casual unrelated chat), politely redirect them toward career/professional topics.
- Be practical, structured, and professional.
- Give actionable advice.

User Query:
${prompt}
`;

const result = await generateGeminiContentStream(restrictedPrompt);
        for await (const chunk of result.stream) {
          const text = chunk.text();

          if (text) {
            fullResponse += text;

            const sseMessage = `data: ${JSON.stringify({ text })}\n\n`;
            controller.enqueue(encoder.encode(sseMessage));
          }
        }

        if (conversationId && fullResponse.trim()) {
          await db.message.create({
            data: {
              conversationId,
              role: "assistant",
              content: fullResponse,
            },
          });

          await db.conversation.update({
            where: {
              id: conversationId,
            },
            data: {
              updatedAt: new Date(),
            },
          });
        }

        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
        controller.close();
      } catch (error) {
        const message =
          error?.response?.error?.message ||
          error?.message ||
          "Unknown Gemini error";

        console.error("Gemini streaming error:", message);

        const errorEvent = `data: ${JSON.stringify({
          error: message,
        })}\n\n`;

        controller.enqueue(encoder.encode(errorEvent));
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-store, must-revalidate",
      Connection: "keep-alive",
    },
  });
}